import click

import kedro_databricks


@click.command()
def command():
    click.echo(f"kedro-databricks version: {kedro_databricks.__version__}")
