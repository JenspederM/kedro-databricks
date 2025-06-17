from __future__ import annotations

import click
from kedro.framework.cli.project import CONF_SOURCE_HELP, PIPELINE_ARG_HELP
from kedro.framework.cli.utils import ENV_HELP
from kedro.framework.startup import ProjectMetadata

import kedro_databricks
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
    """Entry point for Kedro-Databricks commands"""
    pass


@commands.group(name="databricks")
def databricks_commands():
    """Databricks Asset Bundle commands

    These commands are used to manage Databricks Asset Bundles in a Kedro project.
    They allow you to initialize, bundle, deploy, run, and destroy Databricks asset bundles.
    """
    pass


@databricks_commands.command(context_settings=dict(ignore_unknown_options=True))
def version():
    """Display the version of Kedro-Databricks

    This command prints the version of the Kedro-Databricks plugin.
    It is useful for checking the installed version of the plugin in your Kedro project.
    """
    print(f"Kedro-Databricks version: {kedro_databricks.__version__}")  # noqa: T201


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

    This command initializes the Databricks Asset Bundle configuration in the Kedro project.
    The `default_key` is the configuration key to use for Databricks, and the `provider`
    specifies the cloud provider for the Databricks Asset Bundle (e.g., "aws", "azure").

    This command will create a `databricks.yml` file in the `conf/<env>` directory
    and set up the necessary configuration for the Databricks Asset Bundle.

    To see additional options, run `databricks bundle init --help`.

    Args:
        metadata: Project metadata containing project path and other information.
        default_key: The default configuration key to use for Databricks.
        provider: The provider for the Databricks Asset Bundle (e.g., "aws", "azure").
        databricks_args: Additional arguments to pass to the `databricks` CLI.
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
    """Convert kedro pipelines into Databricks asset bundle resources

    This command generates asset bundle resources for the specified Kedro pipeline
    and saves them in the `resources` directory.
    To validate the generated resources, you can run the `databricks bundle validate` command.

    Args:
        metadata: Project metadata containing project path and other information.
        default_key: The default configuration key to use for Databricks.
        env: The environment for the Kedro project (e.g., "dev", "prod").
        conf_source: The source of the Kedro configuration files.
        pipeline: The pipeline to bundle (optional).
        params: Kedro run time parameters in `key1=value1,key2=value2` format (optional).
        overwrite: Whether to overwrite existing resources.
    """
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

    This command deploys the Databricks asset bundle to the specified environment.
    If `--bundle` is specified, it will first bundle the project into resources.
    To see additional options, run `databricks bundle deploy --help`.

    Args:
        metadata: Project metadata containing project path and other information.
        env: The environment for the Kedro project (e.g., "dev", "prod").
        bundle: Whether to bundle the project before deploying.
        conf_source: The source of the Kedro configuration files.
        pipeline: The pipeline to deploy (optional).
        runtime_params: Kedro run time parameters in `key1=value1,key2=value2` format (optional).
        databricks_args: Additional arguments to pass to the `databricks` CLI.
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
    cli.deploy(metadata, env, *databricks_args)


@databricks_commands.command(context_settings=dict(ignore_unknown_options=True))
@click.argument("pipeline")
@click.argument("databricks_args", nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def run(metadata: ProjectMetadata, pipeline: str, databricks_args: list[str]):
    """Run the project on Databricks

    This is a wrapper for the `databricks bundle run` command.
    To see additional options, run `databricks bundle run --help`.

    Args:
        metadata: Project metadata containing project path and other information.
        pipeline: The pipeline to run on Databricks.
        databricks_args: Additional arguments to be passed to the `databricks` CLI.
    """
    cli.run(metadata, pipeline, *databricks_args)


@databricks_commands.command(context_settings=dict(ignore_unknown_options=True))
@click.option("-e", "--env", default=DEFAULT_TARGET, help=ENV_HELP)
@click.argument("databricks_args", nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def destroy(metadata: ProjectMetadata, env: str, databricks_args: list[str]):
    """Destroy the Databricks asset bundle

    This is a wrapper for the `databricks bundle destroy` command.
    To see additional options, run `databricks bundle destroy --help`.

    Args:
        metadata: Project metadata containing project path and other information.
        env: The environment for the Kedro project (e.g., "dev", "prod").
        databricks_args: Additional arguments to be passed to the `databricks` CLI.
    """
    cli.destroy(metadata, env, *databricks_args)
