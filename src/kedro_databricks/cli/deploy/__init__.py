from __future__ import annotations

from kedro.framework.startup import ProjectMetadata

from kedro_databricks.core import DatabricksCli
from kedro_databricks.core.logger import get_logger

log = get_logger("deploy")


def deploy(metadata: ProjectMetadata, env: str, *databricks_args: str):
    """Deploy the Databricks Asset Bundle.

    This function deploys the Databricks Asset Bundle in the current project
    directory. It also creates a Databricks configuration file and a
    Databricks target configuration file.

    Args:
        metadata (ProjectMetadata): The project metadata.
        env (str): The environment to deploy to.
        *databricks_args: Additional arguments to be passed to the `databricks` CLI.

    Raises:
        RuntimeError: If the `databricks` CLI is not installed or the wrong version is used.
    """
    dbcli = DatabricksCli(metadata, env, list(databricks_args))
    dbcli.deploy()
    log.info(f"Deployed Databricks Asset Bundle in {metadata.project_path}")
    dbcli.upload()
    log.info(f"Uploaded project data to Databricks from {metadata.project_path}")
    dbcli.summary()
