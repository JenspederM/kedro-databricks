from collections.abc import Iterable
from typing import Any

from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node

from kedro_databricks.cli.bundle.resource_generator.abstract_resource_generator import (
    AbstractResourceGenerator,
)


class NodeResourceGenerator(AbstractResourceGenerator):
    def _create_workflow_dict(self, name: str, pipeline: Pipeline) -> dict[str, Any]:
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
