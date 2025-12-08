"""Base interfaces and helpers for Databricks resource generation.

This module defines the abstract generator responsible for converting Kedro
pipelines into Databricks jobs according to the Databricks REST API.
Concrete implementations specify how tasks are laid out (e.g., per-node or
per-pipeline).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, MutableMapping
from typing import Any

from kedro.framework.project import pipelines
from kedro.framework.startup import ProjectMetadata
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node

from kedro_databricks.constants import (
    JOB_KEY_ORDER,
    TASK_KEY_ORDER,
)
from kedro_databricks.utilities.common import (
    get_entry_point,
    remove_nulls,
    require_databricks_run_script,
    sanitize_name,
    sort_dict,
)
from kedro_databricks.utilities.logger import get_logger

log = get_logger("bundle").getChild(__name__)


class AbstractResourceGenerator(ABC):
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
        self.remote_conf_dir = f"/${{workspace.file_path}}/{conf_source}"
        self.params = params

    def generate_jobs(
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
        jobs = {}
        pipeline = self.pipelines.get(pipeline_name)
        if pipeline_name and pipeline:
            log.info(f"Generating resources for pipeline '{pipeline_name}'")
            name = self._make_job_name(self.metadata.package_name, pipeline_name)
            jobs[name] = self._create_job(name=name, pipeline=pipeline)
            return jobs
        if pipeline_name:
            raise KeyError(
                f"Pipeline '{pipeline_name}' not found. Available pipelines: {list(self.pipelines.keys())}"
            )

        for pipe_name, pipeline in self.pipelines.items():
            if len(pipeline.nodes) == 0:
                continue
            name = self._make_job_name(self.metadata.package_name, pipe_name)
            job = self._create_job(name=name, pipeline=pipeline)
            log.debug(f"Job '{name}' successfully created.")
            log.debug(job)
            jobs[name] = job

        return jobs

    def _create_job(self, name: str, pipeline: Pipeline) -> dict[str, Any]:
        """Create a Databricks job for a given pipeline.

        Args:
            name (str): name of the pipeline
            pipeline (Pipeline): Kedro pipeline object
            env (str): name of the env to be used by the tasks of the job

        Returns:
            Dict[str, Any]: a Databricks job
        """
        ## Follows the Databricks REST API schema
        ## https://docs.databricks.com/api/workspace/jobs/create
        job = self._create_job_dict(name=name, pipeline=pipeline)
        non_null = remove_nulls(sort_dict(job, JOB_KEY_ORDER))
        if not isinstance(non_null, dict):  # pragma: no cover - this is a type check
            raise RuntimeError("Expected a dict")
        return non_null

    @abstractmethod
    def _create_job_dict(self, name: str, pipeline: Pipeline) -> dict[str, Any]: ...

    def _create_task_with_params(
        self,
        name: str,
        params: list[str],
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

        if require_databricks_run_script():  # pragma: no cover
            entry_point = "databricks_run"
            params = params + ["--package-name", self.metadata.package_name]

        if self.params:
            params = params + ["--params", self.params]

        task = {
            "task_key": sanitize_name(name),
            "depends_on": [
                {"task_key": sanitize_name(dep)}
                for dep in sorted(depends_on, key=lambda dep: dep.name)
            ],
            "python_wheel_task": {
                "package_name": self.metadata.package_name,
                "entry_point": entry_point,
                "parameters": params,
            },
        }

        return sort_dict(task, TASK_KEY_ORDER)

    def _make_job_name(self, package_name: str, pipeline_name: str) -> str:
        """Create a name for the Databricks job.

        Args:
            package_name (str): The name of the Kedro project
            pipeline_name (str): The name of the pipeline

        Returns:
            str: The name of the job
        """
        if pipeline_name == "__default__":
            return package_name
        sanitised_pipeline_name = pipeline_name.replace(".", "_")
        return f"{package_name}_{sanitised_pipeline_name}"
