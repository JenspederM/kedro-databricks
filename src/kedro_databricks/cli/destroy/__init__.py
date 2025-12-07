from kedro.framework.startup import ProjectMetadata

from kedro_databricks.core import DatabricksCli
from kedro_databricks.core.logger import get_logger

log = get_logger("destroy")


def destroy(metadata: ProjectMetadata, env: str, databricks_args: list[str]):
    """Destroy the Databricks Asset Bundle.

    This function destroys the Databricks Asset Bundle in the current project
    directory. It removes the Databricks configuration file and the Databricks target
    configuration file, and deletes the corresponding resources from DBFS.

    Args:
        metadata (ProjectMetadata): The project metadata.
        env (str): The environment to destroy.
        databricks_args: Additional arguments to be passed to the `databricks` CLI.

    Raises:
        RuntimeError: If the `databricks` CLI is not installed or the wrong version is used.
    """
    dbcli = DatabricksCli(metadata, env, databricks_args)
    dbcli.destroy()
    log.info(f"Destroyed Databricks Asset Bundle in {metadata.project_path}")
