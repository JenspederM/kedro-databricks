from kedro_databricks.logger import get_logger
from kedro_databricks.utils import Command, assert_databricks_cli

log = get_logger("destroy")


def destroy(metadata, env: str, *databricks_args):
    assert_databricks_cli()
    cmd = ["databricks", "bundle", "destroy"] + list(databricks_args)
    log.info(
        f"Running `{' '.join(cmd)}` in {metadata.project_path} with Databricks CLI"
    )
    result = Command(cmd, msg="Destroy Databricks bundle", warn=True).run(
        cwd=metadata.project_path
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Failed to destroy Databricks bundle\n" + "\n".join(result.stdout)
        )
    result = Command(
        [
            "databricks",
            "fs",
            "rm",
            "-r",
            f"dbfs:/FileStore/{metadata.project_name.replace('-', '_')}/{env}",
        ],
        msg="Remove Databricks bundle",
    ).run()
    if result.returncode != 0:
        raise RuntimeError(
            "Failed to remove Databricks bundle\n" + "\n".join(result.stdout)
        )
