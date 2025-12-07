from kedro.framework.startup import ProjectMetadata

from kedro_databricks.core import DatabricksCli
from kedro_databricks.core.constants import DEFAULT_TARGET
from kedro_databricks.core.logger import get_logger

log = get_logger("run")


def run(metadata: ProjectMetadata, pipeline, databricks_args: list[str]):
    """Run a Databricks job using the specified pipeline.

    This function runs a Databricks job using the specified pipeline and additional
    arguments. It assumes that the Databricks CLI is installed and configured correctly.

    Args:
        metadata (ProjectMetadata): The project metadata.
        pipeline (str): The name of the pipeline to run.
        databricks_args (list[str]): Additional arguments to be passed to the `databricks` CLI.

    Raises:
        RuntimeError: If the `databricks` CLI is not installed or the job fails to run.
    """
    dbcli = DatabricksCli(metadata, DEFAULT_TARGET, databricks_args)
    dbcli.run(pipeline)
    log.info(
        f"Successfully triggered Databricks job for pipeline '{pipeline}' "
        f"in project {metadata.project_path}"
    )
