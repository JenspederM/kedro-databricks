from __future__ import annotations

import logging
import shutil

import click
from kedro.framework.cli.project import CONF_SOURCE_HELP, PIPELINE_ARG_HELP
from kedro.framework.cli.utils import ENV_HELP
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.bundle import BundleController
from kedro_databricks.constants import (
    DEFAULT_CONF_FOLDER,
    DEFAULT_CONFIG_HELP,
    DEFAULT_CONFIG_KEY,
    DEFAULT_PROVIDER,
    DEFAULT_TARGET,
    NODE_TYPE_MAP,
    PROVIDER_PROMPT,
)
from kedro_databricks.deploy import DeployController
from kedro_databricks.init import InitController
from kedro_databricks.utils.bundle_helpers import require_databricks_run_script


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
    node_type_id = NODE_TYPE_MAP.get(provider)
    if node_type_id is None:
        raise ValueError(f"Invalid provider: {provider}")
    controller = InitController(metadata)
    controller.bundle_init(list(databricks_args))
    controller.create_override_configs(
        node_type_id=node_type_id, default_key=default_key
    )
    controller.update_gitignore()
    if require_databricks_run_script():  # pragma: no cover
        log = logging.getLogger(metadata.package_name)
        log.warning(
            "Kedro version less than 0.19.8 requires a script to run tasks on Databricks. "
        )
        controller.write_databricks_run_script()


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
    if default_key.startswith("_"):  # pragma: no cover
        raise ValueError(
            "Default key cannot start with `_` as this is not recognized by OmegaConf."
        )

    MSG = "Create Asset Bundle Resources"
    controller = BundleController(metadata, env, conf_source, params)
    resources = controller.generate_resources(pipeline, MSG)
    bundle_resources = controller.apply_overrides(resources, default_key)
    controller.save_bundled_resources(bundle_resources, overwrite)


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
    if shutil.which("databricks") is None:  # pragma: no cover
        raise Exception("databricks CLI is not installed")
    controller = DeployController(metadata, env)
    controller.go_to_project()
    controller.validate_databricks_config()
    controller.build_project()
    if bundle is True:
        bundle_controller = BundleController(metadata, env, conf_source, runtime_params)
        workflows = bundle_controller.generate_resources(pipeline)
        bundle_resources = bundle_controller.apply_overrides(
            workflows, DEFAULT_CONFIG_KEY
        )
        bundle_controller.save_bundled_resources(bundle_resources, overwrite=True)
    controller.deploy_project(list(databricks_args))
