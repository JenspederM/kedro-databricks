import click
from kedro.framework.cli.utils import ENV_HELP
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.constants import DEFAULT_ENV
from kedro_databricks.utilities.databricks_cli import DatabricksCli
from kedro_databricks.utilities.logger import get_logger

log = get_logger("init")


@click.command()
@click.argument("pipeline", default="", nargs=1)
@click.option("-e", "--env", default=DEFAULT_ENV, help=ENV_HELP)
@click.argument("databricks_args", nargs=-1, type=click.UNPROCESSED)
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
