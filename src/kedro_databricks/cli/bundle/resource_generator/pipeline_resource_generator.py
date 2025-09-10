"""Pipeline-level Databricks resource generator.

Creates a Databricks workflow with a single task that runs an entire Kedro
pipeline in one go.
"""

from typing import Any

from kedro.pipeline import Pipeline

from kedro_databricks.cli.bundle.resource_generator.abstract_resource_generator import (
    AbstractResourceGenerator,
)


class PipelineResourceGenerator(AbstractResourceGenerator):
    """Generate a workflow with a single task for the whole pipeline."""

    def _create_workflow_dict(self, name: str, pipeline: Pipeline) -> dict[str, Any]:
        """Build the workflow payload for a pipeline-based job.

        Args:
            name (str): The workflow/job name.
            pipeline (Pipeline): The Kedro pipeline to run as a single task.

        Returns:
            dict[str, Any]: A Databricks workflow payload containing one task.
        """
        return {"name": name, "tasks": [self._create_pipeline_task(name)]}

    def _create_pipeline_task(self, name: str) -> dict[str, Any]:
        """Create the single task that executes the full pipeline.

        Args:
            name (str): The pipeline (and task) name.

        Returns:
            dict[str, Any]: A Databricks task definition for the pipeline run.
        """
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
