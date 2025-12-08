from __future__ import annotations

import click
from kedro.framework.cli.project import (
    CONF_SOURCE_HELP,
    PARAMS_ARG_HELP,
    PIPELINE_ARG_HELP,
)
from kedro.framework.cli.utils import ENV_HELP
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.commands.bundle import command as bundle_command
from kedro_databricks.constants import (
    DEFAULT_CONF_FOLDER,
    DEFAULT_CONFIG_GENERATOR,
    DEFAULT_CONFIG_GENERATOR_HELP,
    DEFAULT_CONFIG_KEY,
    DEFAULT_CONFIG_KEY_HELP,
    DEFAULT_ENV,
)
from kedro_databricks.utilities.databricks_cli import DatabricksCli
from kedro_databricks.utilities.logger import get_logger

log = get_logger("deploy")


@click.command()
@click.option(
    "-e",
    "--env",
    default=DEFAULT_ENV,
    help=ENV_HELP,
)
@click.option(
    "-b",
    "--bundle/--no-bundle",
    default=False,
    help="Bundle the project before deploying",
)
@click.option(
    "-c",
    "--conf-source",
    default=DEFAULT_CONF_FOLDER,
    help=CONF_SOURCE_HELP + " (forwarded to the bundle command).",
)
@click.option(
    "-g",
    "--resource-generator",
    default=DEFAULT_CONFIG_GENERATOR,
    help=DEFAULT_CONFIG_GENERATOR_HELP + " (forwarded to the bundle command).",
)
@click.option(
    "-p",
    "--pipeline",
    default=None,
    help=PIPELINE_ARG_HELP + " (forwarded to the bundle command).",
)
@click.option(
    "-r",
    "--runtime-params",
    default=None,
    help=PARAMS_ARG_HELP + " (forwarded to the bundle command).",
)
@click.option(
    "--default-key",
    type=str,
    default=DEFAULT_CONFIG_KEY,
    help=DEFAULT_CONFIG_KEY_HELP + " (forwarded to the bundle command).",
)
@click.argument(
    "databricks_args",
    nargs=-1,
    type=click.UNPROCESSED,
)
@click.pass_context
def command(
    ctx: click.Context,
    env: str,
    bundle: bool,
    default_key: str,
    conf_source: str,
    resource_generator: str,
    pipeline: str | None,
    runtime_params: str | None,
    databricks_args: tuple[str, ...],
):
    """Deploy the Databricks Asset Bundle.

    This function deploys the Databricks Asset Bundle in the current project
    directory. It also creates a Databricks configuration file and a
    Databricks target configuration file.
    """
    metadata = ctx.obj
    if not isinstance(metadata, ProjectMetadata):
        raise TypeError("Project metadata is not available in the context.")
    if bundle:
        ctx.invoke(
            bundle_command,
            env=env,
            default_key=default_key,
            conf_source=conf_source,
            resource_generator=resource_generator,
            pipeline=pipeline,
            params=runtime_params,
            overwrite=True,
        )
    dbcli = DatabricksCli(metadata, env=env, additional_args=list(databricks_args))
    dbcli.deploy()
    log.info(f"Deployed Databricks Asset Bundle in {metadata.project_path}")
    dbcli.upload()
    log.info(f"Uploaded project data to Databricks from {metadata.project_path}")
    dbcli.summary()
