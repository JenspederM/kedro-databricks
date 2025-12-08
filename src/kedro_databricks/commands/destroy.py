import click
from kedro.framework.cli.utils import ENV_HELP
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.constants import DEFAULT_ENV
from kedro_databricks.utilities.databricks_cli import DatabricksCli


@click.command()
@click.option(
    "-e",
    "--env",
    default=DEFAULT_ENV,
    help=ENV_HELP,
)
@click.argument(
    "databricks_args",
    nargs=-1,
    type=click.UNPROCESSED,
)
@click.pass_obj
def command(metadata: ProjectMetadata, env: str, databricks_args: tuple[str, ...]):
    """Databricks Asset Bundle Destroy commands"""
    dbcli = DatabricksCli(
        metadata=metadata,
        env=env,
        additional_args=list(databricks_args),
    )
    dbcli.destroy()
