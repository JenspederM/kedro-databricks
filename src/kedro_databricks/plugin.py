from __future__ import annotations

import click
from kedro.framework.cli.project import CONF_SOURCE_HELP, PIPELINE_ARG_HELP
from kedro.framework.cli.utils import ENV_HELP
from kedro.framework.startup import ProjectMetadata

from kedro_databricks import cli
from kedro_databricks.constants import (
    DEFAULT_CONF_FOLDER,
    DEFAULT_CONFIG_HELP,
    DEFAULT_CONFIG_KEY,
    DEFAULT_PROVIDER,
    DEFAULT_TARGET,
    PROVIDER_PROMPT,
)


@click.group(name="Kedro-Databricks")
def commands():
    pass


@commands.group(name="databricks")
def databricks_commands():
    """Run project with Databricks"""
    pass


@databricks_commands.command(context_settings=dict(ignore_unknown_options=True))
@click.option(
    "-d", "--default-key", default=DEFAULT_CONFIG_KEY, help=DEFAULT_CONFIG_HELP
)
@click.option("--provider", prompt=PROVIDER_PROMPT, default=DEFAULT_PROVIDER)
@click.argument("databricks_args", nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def init(
    metadata: ProjectMetadata,
    default_key: str,
    provider: str,
    databricks_args: list[str],
):
    """Initialize Databricks Asset Bundle configuration

    `databricks_args` are additional arguments to be passed to the `databricks` CLI.
    """
    cli.init(metadata, provider, default_key, *databricks_args)


@databricks_commands.command()
@click.option(
    "-d", "--default-key", default=DEFAULT_CONFIG_KEY, help=DEFAULT_CONFIG_HELP
)
@click.option("-e", "--env", default=DEFAULT_TARGET, help=ENV_HELP)
@click.option("-c", "--conf-source", default=DEFAULT_CONF_FOLDER, help=CONF_SOURCE_HELP)
@click.option("-p", "--pipeline", default=None, help=PIPELINE_ARG_HELP)
@click.option(
    "-r",
    "--params",
    default=None,
    help="Kedro run time params in `key1=value1,key2=value2` format",
)
@click.option(
    "--overwrite",
    default=False,
    is_flag=True,
    show_default=True,
    help="Overwrite the existing resources",
)
@click.pass_obj
def bundle(
    metadata: ProjectMetadata,
    default_key: str,
    env: str,
    conf_source: str,
    pipeline: str | None,
    params: str | None,
    overwrite: bool,
):
    """Convert kedro pipelines into Databricks asset bundle resources"""
    cli.bundle(
        metadata=metadata,
        env=env,
        conf_source=conf_source,
        pipeline_name=pipeline,
        default_key=default_key,
        params=params,
        overwrite=overwrite,
    )


@databricks_commands.command(context_settings=dict(ignore_unknown_options=True))
@click.option("-e", "--env", default=DEFAULT_TARGET, help=ENV_HELP)
@click.option(
    "-b",
    "--bundle/--no-bundle",
    default=False,
    help="Bundle the project before deploying",
)
@click.option("-c", "--conf-source", default=DEFAULT_CONF_FOLDER, help=CONF_SOURCE_HELP)
@click.option("-p", "--pipeline", default=None, help=PIPELINE_ARG_HELP)
@click.option(
    "-r",
    "--runtime-params",
    default=None,
    help="Kedro run time params in `key1=value1,key2=value2` format",
)
@click.argument("databricks_args", nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def deploy(
    metadata: ProjectMetadata,
    env: str,
    bundle: bool,
    conf_source: str,
    pipeline: str,
    runtime_params: str | None,
    databricks_args: list[str],
):
    """Deploy the asset bundle to Databricks

    `databricks_args` are additional arguments to be passed to the `databricks` CLI.
    """
    if bundle:
        cli.bundle(
            metadata=metadata,
            env=env,
            pipeline_name=pipeline,
            conf_source=conf_source,
            default_key=DEFAULT_CONFIG_KEY,
            params=runtime_params,
            overwrite=True,
        )
    cli.deploy(
        metadata=metadata,
        env=env,
        *databricks_args,
    )


@databricks_commands.command(context_settings=dict(ignore_unknown_options=True))
@click.argument("pipeline")
@click.argument("databricks_args", nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def run(metadata: ProjectMetadata, pipeline, databricks_args: list[str]):
    """Run the project on Databricks

    This is a wrapper for the `databricks bundle run` command.
    To see available options, run `databricks bundle run --help`.

    `databricks_args` are additional arguments to be passed to the `databricks` CLI.
    """
    cli.run(
        metadata=metadata,
        pipeline=pipeline,
        *databricks_args,
    )


@databricks_commands.command(context_settings=dict(ignore_unknown_options=True))
@click.option("-e", "--env", default=DEFAULT_TARGET, help=ENV_HELP)
@click.argument("databricks_args", nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def destroy(metadata: ProjectMetadata, env, databricks_args: list[str]):
    """Destroy the Databricks asset bundle

    This is a wrapper for the `databricks bundle destroy` command.
    To see available options, run `databricks bundle destroy --help`.

    `databricks_args` are additional arguments to be passed to the `databricks` CLI.
    """
    cli.destroy(metadata=metadata, env=env, *databricks_args)
