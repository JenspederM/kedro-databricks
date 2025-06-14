from __future__ import annotations

from collections.abc import Iterable, MutableMapping
from typing import Any

from kedro.framework.project import pipelines
from kedro.framework.startup import ProjectMetadata
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node

from kedro_databricks.cli.bundle.utils import get_entry_point, remove_nulls, sort_dict
from kedro_databricks.constants import TASK_KEY_ORDER, WORKFLOW_KEY_ORDER
from kedro_databricks.logger import get_logger
from kedro_databricks.utils import make_workflow_name, require_databricks_run_script

log = get_logger("bundle").getChild(__name__)


class ResourceGenerator:
    """Generates Databricks resources for the given pipelines.

    Finds all pipelines in the project and generates Databricks asset bundle resources
    for each according to the Databricks REST API
    """

    def __init__(
        self,
        metadata: ProjectMetadata,
        env: str,
        conf_source: str = "conf",
        params: str | None = None,
    ) -> None:
        self.metadata = metadata
        self.env = env
        self.pipelines: MutableMapping = pipelines
        self.remote_conf_dir = f"/Workspace/${{workspace.file_path}}/{conf_source}"
        self.params = params

    def generate_resources(
        self, pipeline_name: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """Generate Databricks resources for the given pipelines.

        Finds all pipelines in the project and generates Databricks asset bundle resources
        for each according to the Databricks REST API

        Args:
            pipeline_name (str | None): The name of the pipeline for which Databricks asset bundle resources should be generated.
                If None, generates all pipelines.

        Returns:
            dict[str, dict[str, Any]]: A dictionary of pipeline names and their Databricks resources
        """
        workflows = {}
        pipeline = self.pipelines.get(pipeline_name)
        if pipeline_name and pipeline:
            log.info(f"Generating resources for pipeline '{pipeline_name}'")
            name = make_workflow_name(self.metadata.package_name, pipeline_name)
            workflows[name] = self._create_workflow(name=name, pipeline=pipeline)
            return self._workflows_to_resources(workflows)
        if pipeline_name:
            raise KeyError(
                f"Pipeline '{pipeline_name}' not found. Available pipelines: {list(self.pipelines.keys())}"
            )

        for pipe_name, pipeline in self.pipelines.items():
            if len(pipeline.nodes) == 0:
                continue
            name = make_workflow_name(self.metadata.package_name, pipe_name)
            workflow = self._create_workflow(name=name, pipeline=pipeline)
            log.debug(f"Workflow '{name}' successfully created.")
            log.debug(workflow)
            workflows[name] = workflow

        return self._workflows_to_resources(workflows)

    def _workflows_to_resources(
        self, workflows: dict[str, dict[str, Any]]
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
        log.debug(resources)
        log.info("Databricks resources successfully generated.")
        return resources

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
        entry_point = get_entry_point(self.metadata.project_name)
        params = [
            "--nodes",
            name,
            "--conf-source",
            self.remote_conf_dir,
            "--env",
            "${var.environment}",
        ]

        if require_databricks_run_script():  # pragma: no cover
            entry_point = "databricks_run"
            params = params + ["--package-name", self.metadata.package_name]

        if self.params:
            params = params + ["--params", self.params]

        task = {
            "task_key": name.replace(".", "_"),
            "libraries": [{"whl": "../dist/*.whl"}],
            "depends_on": [
                {"task_key": dep.name.replace(".", "_")}
                for dep in sorted(depends_on, key=lambda dep: dep.name)
            ],
            "python_wheel_task": {
                "package_name": self.metadata.package_name,
                "entry_point": entry_point,
                "parameters": params,
            },
        }

        return sort_dict(task, TASK_KEY_ORDER)
