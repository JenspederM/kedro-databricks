from __future__ import annotations

import warnings

import click
from kedro.framework.startup import ProjectMetadata

import kedro_databricks.commands._options as option
from kedro_databricks.commands.bundle import command as bundle_command
from kedro_databricks.utilities.databricks_cli import DatabricksCli
from kedro_databricks.utilities.logger import get_logger

log = get_logger("deploy")

option.params


@click.command()
@option.env
@option.bundle
@option.default_key
@option.resource_generator
@option.conf_source
@option.pipeline
@option.params
@option.databricks_args
@option.runtime_params
@click.pass_context
def command(
    ctx: click.Context,
    env: str,
    bundle: bool,
    default_key: str,
    resource_generator: str,
    conf_source: str,
    pipeline: str | None,
    params: str | None,
    runtime_params: str | None,
    databricks_args: tuple[str, ...],
):
    """Deploy the Databricks Asset Bundle.

    This function deploys the Databricks Asset Bundle in the current project
    directory. It also creates a Databricks configuration file and a
    Databricks target configuration file.
    """
    if runtime_params:
        warnings.warn(
            "'--runtime-params' has been renamed to '--params' to be consistent with `kedro databricks bundle`"
        )
        params = runtime_params
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
            params=params,
            overwrite=True,
        )
    dbcli = DatabricksCli(metadata, env=env, additional_args=list(databricks_args))
    dbcli.deploy()
    log.info(f"Deployed Databricks Asset Bundle in {metadata.project_path}")
    dbcli.upload()
    log.info(f"Uploaded project data to Databricks from {metadata.project_path}")
    dbcli.summary()
