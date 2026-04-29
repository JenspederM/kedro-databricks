import click
from kedro.framework.startup import ProjectMetadata

import kedro_databricks.commands._options as option
from kedro_databricks.utilities.databricks_cli import DatabricksCli
from kedro_databricks.utilities.logger import get_logger

log = get_logger("init")


@click.command()
@option.pipeline_arg
@option.env
@option.databricks_args
@click.pass_obj
def command(
    metadata: ProjectMetadata,
    env: str,
    pipeline: str,
    databricks_args: tuple[str, ...],
):
    """Databricks Asset Bundle Run commands"""
    # If the first argument starts with '--', it means no pipeline was provided
    # This is to handle the case where the user wants to pass only options to the CLI
    # e.g. `databricks bundle run -- --profile prod` will run the default pipeline with the prod profile
    if pipeline.startswith("--"):
        databricks_args = (pipeline,) + databricks_args
        pipeline = ""

    if not pipeline:
        pipeline = metadata.package_name

    dbcli = DatabricksCli(
        metadata=metadata,
        env=env,
        additional_args=list(databricks_args),
    )
    dbcli.run(pipeline)
    log.info(
        f"Successfully triggered Databricks job for pipeline '{pipeline}' in project {metadata.project_path}"
    )
