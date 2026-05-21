import click

import kedro_databricks


@click.command(name="version")
def command():
    click.echo(f"kedro-databricks version: {kedro_databricks.__version__}")
