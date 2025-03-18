from __future__ import annotations

import logging
from collections.abc import Iterable, MutableMapping
from pathlib import Path
from typing import Any

import yaml
from kedro.config import MissingConfigException
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import ProjectMetadata
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node

from kedro_databricks.constants import TASK_KEY_ORDER, WORKFLOW_KEY_ORDER
from kedro_databricks.utils.bundle_helpers import (
    get_entry_point,
    remove_nulls,
    require_databricks_run_script,
    sort_dict,
)
from kedro_databricks.utils.common import make_workflow_name
from kedro_databricks.utils.override_resources import override_resources


class BundleController:
    def __init__(
        self,
        metadata: ProjectMetadata,
        env: str,
        config_dir: str = "conf",
        runtime_params: str | None = None,
    ) -> None:
        """Create a new instance of the BundleController.

        Args:
            metadata (ProjectMetadata): The metadata of the project
            env (str): The name of the kedro environment
            config_dir (str, optional): The name of the configuration directory. Defaults to None.
        """

        self.metadata: ProjectMetadata = metadata
        self.env: str = env
        self._conf: str = config_dir
        self.project_name: str = metadata.project_name
        self.project_path: Path = metadata.project_path
        self.package_name: str = metadata.package_name
        self.pipelines: MutableMapping = pipelines
        self.log: logging.Logger = logging.getLogger(self.package_name)
        self.remote_conf_dir: str = "${workspace.file_path}/" + config_dir
        self.local_conf_dir: Path = self.metadata.project_path / config_dir / env
        self.conf: dict[str, Any] = self._load_env_config(MSG="Loading configuration")
        self.runtime_params = runtime_params

    def _workflows_to_resources(
        self, workflows: dict[str, dict[str, Any]], MSG: str = ""
    ) -> dict[str, dict[str, Any]]:
        """Convert Databricks workflows to Databricks resources.

        Args:
            workflows (Dict[str, Dict[str, Any]]): A dictionary of Databricks workflows

        Returns:
            Dict[str, Dict[str, Any]]: A dictionary of Databricks resources
        """
        resources = {
            name: {"resources": {"jobs": {name: wf}}} for name, wf in workflows.items()
        }
        self.log.debug(resources)
        self.log.info(f"{MSG}: Databricks resources successfully generated.")
        return resources

    def generate_resources(
        self, pipeline_name: str | None = None, MSG: str = ""
    ) -> dict[str, dict[str, Any]]:
        """Generate Databricks resources for the given pipelines.

        Finds all pipelines in the project and generates Databricks asset bundle resources
        for each according to the Databricks REST API

        Args:
            env (str): The name of the kedro environment to be used by the workflow
            pipeline_name (str | None): The name of the pipeline for which Databricks asset bundle resources should be generated.
            If None, generates all pipelines.
            MSG (str): The message to display

        Returns:
            dict[str, dict[str, Any]]: A dictionary of pipeline names and their Databricks resources
        """
        workflows = {}
        pipeline = self.pipelines.get(pipeline_name)
        if pipeline_name and pipeline:
            self.log.info(f"Generating resources for pipeline '{pipeline_name}'")
            name = make_workflow_name(self.package_name, pipeline_name)
            workflows[name] = self._create_workflow(name=name, pipeline=pipeline)
            return self._workflows_to_resources(workflows, MSG)

        for pipe_name, pipeline in self.pipelines.items():
            if len(pipeline.nodes) == 0:
                continue
            name = make_workflow_name(self.package_name, pipe_name)
            workflow = self._create_workflow(name=name, pipeline=pipeline)
            self.log.debug(f"Workflow '{name}' successfully created.")
            self.log.debug(workflow)
            workflows[name] = workflow

        return self._workflows_to_resources(workflows, MSG)

    def apply_overrides(self, resources: dict[str, Any], default_key):
        """Apply overrides to the Databricks resources.

        Args:
            resources (Dict[str, Any]): dictionary of Databricks resources
            default_key (str, optional): default key to use for overrides

        Returns:
            Dict[str, Any]: dictionary of Databricks resources with overrides applied
        """
        result = {}
        for name, resource in resources.items():
            result[name] = override_resources(resource, self.conf, default_key)
        return result

    def _load_env_config(self, MSG: str = "") -> dict[str, Any]:
        """Load the Databricks configuration for the given environment.

        Args:
            env (str): The name of the kedro environment
            MSG (str): The message to display

        Returns:
            dict[str, Any]: The Databricks configuration for the given environment
        """
        # If the configuration directory does not exist, Kedro will not load any configuration
        if not self.local_conf_dir.exists():
            self.log.warning(
                f"{MSG}: Creating {self.local_conf_dir.relative_to(self.project_path)}"
            )
            self.local_conf_dir.mkdir(parents=True)

        with KedroSession.create(
            project_path=self.project_path, env=self.env
        ) as session:
            config_loader = session._get_config_loader()
            # Backwards compatibility for ConfigLoader that does not support `config_patterns`
            if not hasattr(config_loader, "config_patterns"):
                return config_loader.get(
                    "databricks*", "databricks/**"
                )  # pragma: no cover

            # Set the default pattern for `databricks` if not provided in `settings.py`
            if "databricks" not in config_loader.config_patterns.keys():  # type: ignore
                config_loader.config_patterns.update(  # pragma: no cover # type: ignore
                    {"databricks": ["databricks*", "databricks/**"]}
                )

            assert "databricks" in config_loader.config_patterns.keys()  # type: ignore

            # Load the config
            try:
                return config_loader["databricks"]
            except MissingConfigException:  # pragma: no cover
                self.log.warning("No Databricks configuration found.")
                return {}

    def _create_workflow(self, name: str, pipeline: Pipeline) -> dict[str, Any]:
        """Create a Databricks workflow for a given pipeline.

        Args:
            name (str): name of the pipeline
            pipeline (Pipeline): Kedro pipeline object
            env (str): name of the env to be used by the tasks of the workflow

        Returns:
            Dict[str, Any]: a Databricks workflow
        """
        ## Follows the Databricks REST API schema
        ## https://docs.databricks.com/api/workspace/jobs/create
        workflow = {
            "name": name,
            "tasks": [
                self._create_task(node.name, depends_on=deps)
                for node, deps in sorted(pipeline.node_dependencies.items())
            ],
        }
        non_null = remove_nulls(sort_dict(workflow, WORKFLOW_KEY_ORDER))
        if not isinstance(non_null, dict):  # pragma: no cover - this is a type check
            raise RuntimeError("Expected a dict")
        return non_null

    def _create_task(
        self,
        name: str,
        depends_on: Iterable[Node],
    ) -> dict[str, Any]:
        """Create a Databricks task for a given node.

        Args:
            name (str): name of the node
            depends_on (List[Node]): list of nodes that the task depends on
            env (str): name of the env to be used by the task

        Returns:
            Dict[str, Any]: a Databricks task
        """
        ## Follows the Databricks REST API schema. See "tasks" in the link below
        ## https://docs.databricks.com/api/workspace/jobs/create
        entry_point = get_entry_point(self.project_name)
        params = [
            "--nodes",
            name,
            "--conf-source",
            self.remote_conf_dir,
            "--env",
            self.env,
        ]

        if require_databricks_run_script():  # pragma: no cover
            entry_point = "databricks_run"
            params = params + ["--package-name", self.package_name]

        if self.runtime_params:
            params = params + ["--params", self.runtime_params]

        task = {
            "task_key": name.replace(".", "_"),
            "libraries": [{"whl": "../dist/*.whl"}],
            "depends_on": [
                {"task_key": dep.name.replace(".", "_")}
                for dep in sorted(depends_on, key=lambda dep: dep.name)
            ],
            "python_wheel_task": {
                "package_name": self.package_name,
                "entry_point": entry_point,
                "parameters": params,
            },
        }

        return sort_dict(task, TASK_KEY_ORDER)

    def save_bundled_resources(
        self, resources: dict[str, dict[str, Any]], overwrite: bool = False
    ):
        """Save the generated resources to the project directory.

        Args:
            resources (Dict[str, Dict[str, Any]]): A dictionary of pipeline names and their Databricks resources
            metadata (ProjectMetadata): The metadata of the project
            overwrite (bool): Whether to overwrite existing resources
        """
        resources_dir = self.project_path / "resources"
        resources_dir.mkdir(exist_ok=True, parents=True)
        for name, resource in resources.items():
            MSG = f"Writing resource '{name}'"
            p = resources_dir / f"{name}.yml"

            if p.exists() and not overwrite:  # pragma: no cover
                self.log.warning(
                    f"{MSG}: {p.relative_to(self.project_path)} already exists."
                    " Use --overwrite to replace."
                )
                continue

            with open(p, "w") as f:
                self.log.info(f"{MSG}: Wrote {p.relative_to(self.project_path)}")
                yaml.dump(
                    resource, f, default_flow_style=False, indent=4, sort_keys=False
                )
