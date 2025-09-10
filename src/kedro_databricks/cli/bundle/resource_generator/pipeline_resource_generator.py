from typing import Any

from kedro.pipeline import Pipeline

from kedro_databricks.cli.bundle.resource_generator.abstract_resource_generator import (
    AbstractResourceGenerator,
)


class PipelineResourceGenerator(AbstractResourceGenerator):
    def _create_workflow_dict(self, name: str, pipeline: Pipeline) -> dict[str, Any]:
        return {"name": name, "tasks": [self._create_pipeline_task(name)]}

    def _create_pipeline_task(self, name: str) -> dict[str, Any]:
        return self._create_task_with_params(
            name=name,
            params=[
                "--pipeline",
                name,
                "--conf-source",
                self.remote_conf_dir,
                "--env",
                "${var.environment}",
            ],
            depends_on=[],
        )
