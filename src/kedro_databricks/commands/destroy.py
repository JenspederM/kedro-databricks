import click
from kedro.framework.startup import ProjectMetadata

import kedro_databricks.commands._options as option
from kedro_databricks.utilities.databricks_cli import DatabricksCli


@click.command()
@option.env
@option.databricks_args
@click.pass_obj
def command(metadata: ProjectMetadata, env: str, databricks_args: tuple[str, ...]):
    """Databricks Asset Bundle Destroy commands"""
    dbcli = DatabricksCli(
        metadata=metadata,
        env=env,
        additional_args=list(databricks_args),
    )
    dbcli.destroy()
