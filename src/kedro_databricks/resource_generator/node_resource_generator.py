"""Node-level Databricks resource generator.

Creates a Databricks job where each Kedro node becomes an individual task
with appropriate dependencies derived from the pipeline graph.
"""

from collections.abc import Iterable
from typing import Any

from kedro.framework.session import KedroSession
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from kedro_telemetry.plugin import ProjectMetadata

from kedro_databricks.resource_generator.abstract_resource_generator import (
    AbstractResourceGenerator,
)
from kedro_databricks.resource_generator.exceptions import MemoryDatasetError


class NodeResourceGenerator(AbstractResourceGenerator):
    """Generate a job with one Databricks task per Kedro node."""

    def __init__(
        self,
        session: KedroSession,
        metadata: ProjectMetadata,
        conf_source: str = "conf",
        params: str | None = None,
    ) -> None:
        super().__init__(session, metadata, conf_source, params)
        undeclared_datasets = self._get_memory_datasets()
        if len(undeclared_datasets) > 0:
            raise MemoryDatasetError(self, undeclared_datasets)

    def _create_job_dict(
        self, name: str, pipeline: Pipeline, pipeline_name: str
    ) -> dict[str, Any]:  # noqa: ARG002
        """Build the job payload for a node-based job.

        Args:
            name (str): The job name.
            pipeline (Pipeline): The Kedro pipeline to convert.
            pipeline_name (str): Unused parameter for compatibility with the abstract method.

        Returns:
            dict[str, Any]: A Databricks job payload containing per-node tasks.
        """
        return {
            "name": name,
            "tasks": [
                self._create_task(node, depends_on=deps)
                for node, deps in sorted(pipeline.node_dependencies.items())
            ],
        }

    def _create_task(
        self,
        node: Node,
        depends_on: Iterable[Node],
    ) -> dict[str, Any]:
        """Create a task for a specific node with dependency wiring.

        Args:
            node (Node): The Kedro node to run.
            depends_on (Iterable[Node]): Upstream nodes this task depends on.

        Returns:
            dict[str, Any]: A Databricks task definition for the node.
        """
        return self._create_task_with_params(
            name=node.name,
            params=[
                "--nodes",
                node.name,
                "--conf-source",
                self.remote_conf_dir,
                "--env",
                "${var.environment}",
            ],
            depends_on=depends_on,
        )
