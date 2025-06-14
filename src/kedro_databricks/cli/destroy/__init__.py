from kedro.framework.startup import ProjectMetadata

from kedro_databricks.logger import get_logger
from kedro_databricks.utils import Command, assert_databricks_cli

log = get_logger("destroy")


def destroy(metadata: ProjectMetadata, env: str, *databricks_args: str):
    """Destroy the Databricks Asset Bundle.

    This function destroys the Databricks Asset Bundle in the current project
    directory. It removes the Databricks configuration file and the Databricks target
    configuration file, and deletes the corresponding resources from DBFS.

    Args:
        metadata (ProjectMetadata): The project metadata.
        env (str): The environment to destroy.
        *databricks_args: Additional arguments to be passed to the `databricks` CLI.

    Raises:
        RuntimeError: If the `databricks` CLI is not installed or the wrong version is used.
    """
    assert_databricks_cli()
    destroy_cmd = ["databricks", "bundle", "destroy"] + list(databricks_args)
    Command(destroy_cmd, log=log, warn=True).run(cwd=metadata.project_path)
    Command(
        [
            "databricks",
            "fs",
            "rm",
            "-r",
            f"dbfs:/FileStore/{metadata.package_name}/{env}",
        ],
        log=log,
        warn=True,
    ).run()
