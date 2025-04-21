from kedro.framework.startup import ProjectMetadata

from kedro_databricks.logger import get_logger
from kedro_databricks.utils import Command, assert_databricks_cli

log = get_logger("destroy")


def destroy(metadata: ProjectMetadata, env: str, *databricks_args):
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
