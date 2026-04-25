"""Pipeline-level Databricks resource generator.

Creates a Databricks job with a single task that runs an entire Kedro
pipeline in one go.
"""

from typing import Any

from kedro.pipeline import Pipeline

from kedro_databricks.utilities.resource_generator.abstract_resource_generator import (
    AbstractResourceGenerator,
)


class PipelineResourceGenerator(AbstractResourceGenerator):
    """Generate a job with a single task for the whole pipeline."""

    def can_handle_memory_datasets(self) -> bool:
        return True

    def _create_job_dict(
        self,
        name: str,
        pipeline: Pipeline,
        pipeline_name: str,  # noqa: ARG002
    ) -> dict[str, Any]:
        """Build the job payload for a pipeline-based job.

        Args:
            name (str): The job name.
            pipeline (Pipeline): Unused parameter for compatibility with the abstract method.
            pipeline_name (str): The name of the pipeline.

        Returns:
            dict[str, Any]: A Databricks job payload containing one task.
        """
        if pipeline_name is None:
            raise ValueError(
                "Pipeline name must be provided to create a job dict when --pipeline is used."
            )
        return {
            "name": name,
            "tasks": [self._create_pipeline_task(pipeline_name)],
        }

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
