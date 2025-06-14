from kedro.framework.startup import ProjectMetadata

from kedro_databricks.logger import get_logger
from kedro_databricks.utils import Command, assert_databricks_cli

log = get_logger("run")


def run(metadata: ProjectMetadata, pipeline: str, *databricks_args: str):
    """Run a Databricks job using the specified pipeline.

    This function runs a Databricks job using the specified pipeline and additional
    arguments. It assumes that the Databricks CLI is installed and configured correctly.

    Args:
        metadata (ProjectMetadata): The project metadata.
        pipeline (str): The name of the pipeline to run.
        *databricks_args: Additional arguments to be passed to the `databricks` CLI.

    Raises:
        RuntimeError: If the `databricks` CLI is not installed or the job fails to run.
    """
    assert_databricks_cli()
    cmd = ["databricks", "bundle", "run", pipeline] + list(databricks_args)
    log.info(f"Running `{' '.join(cmd)}` in {metadata.project_path}")
    result = Command(cmd, log=log, warn=True).run(cwd=metadata.project_path)
    if result.returncode != 0:  # pragma: no cover
        raise RuntimeError("Failed to run Databricks job\n" + "\n".join(result.stdout))
