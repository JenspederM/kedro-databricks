from __future__ import annotations

import logging
import shutil

import click
from kedro.framework.cli.utils import ENV_HELP
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.bundle import (
    BundleController,
)
from kedro_databricks.deploy import DeployController
from kedro_databricks.init import InitController
from kedro_databricks.utils import require_databricks_run_script

DEFAULT_RUN_ENV = "local"
DEFAULT_CONF_FOLDER = "conf"
DEFAULT_CONFIG_KEY = "default"
DEFAULT_CONFIG_HELP = "Set the key for the default configuration"
CONF_HELP = "Set the conf folder. Default to `conf`."
_PROVIDER_PROMPT = """
Please select your cloud provider:
1. Azure
2. AWS
3. GCP
"""
_PROVIDER_MAP = {
    "1": "azure",
    "2": "aws",
    "3": "gcp",
}


@click.group(name="Kedro-Databricks")
def commands():
    pass


@commands.group(name="databricks")
def databricks_commands():
    """Run project with Databricks"""
    pass


@databricks_commands.command()
@click.option("-d", "--default", default=DEFAULT_CONFIG_KEY, help=DEFAULT_CONFIG_HELP)
@click.option("--provider", prompt=_PROVIDER_PROMPT, default="1")
@click.pass_obj
def init(
    metadata: ProjectMetadata,
    default: str,
    provider: str,
):
    """Initialize Databricks Asset Bundle configuration"""
    provider_name = _PROVIDER_MAP.get(provider)
    controller = InitController(metadata)
    controller.bundle_init()
    controller.write_kedro_databricks_config(default, provider_name)
    if require_databricks_run_script():  # pragma: no cover
        log = logging.getLogger(metadata.package_name)
        log.warning(
            "Kedro version less than 0.19.8 requires a script to run tasks on Databricks. "
        )
        controller.write_databricks_run_script()
    controller.substitute_catalog_paths()


@databricks_commands.command()
@click.option("-d", "--default", default=DEFAULT_CONFIG_KEY, help=DEFAULT_CONFIG_HELP)
@click.option("-e", "--env", default=DEFAULT_RUN_ENV, help=ENV_HELP)
@click.option("-c", "--conf", default=DEFAULT_CONF_FOLDER, help=CONF_HELP)
@click.option("-p", "--pipeline", default=None, help="Bundle a single pipeline")
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
    default: str,
    env: str,
    conf: str,
    pipeline: str | None,
    overwrite: bool,
):
    """Convert kedro pipelines into Databricks asset bundle resources"""
    if default.startswith("_"):  # pragma: no cover
        raise ValueError(
            "Default key cannot start with `_` as this is not recognized by OmegaConf."
        )

    MSG = "Create Asset Bundle Resources"
    controller = BundleController(metadata, env, conf)
    resources = controller.generate_resources(pipeline, MSG)
    bundle_resources = controller.apply_overrides(resources, default)
    controller.save_bundled_resources(bundle_resources, overwrite)


@databricks_commands.command()
@click.option("-e", "--env", default=DEFAULT_RUN_ENV, help=ENV_HELP)
@click.option(
    "-t",
    "--target",
    default=None,
    help="Databricks target environment. Defaults to the `env` value.",
)
@click.option(
    "-b",
    "--bundle/--no-bundle",
    default=False,
    help="Bundle the project before deploying",
)
@click.option("-c", "--conf", default=DEFAULT_CONF_FOLDER, help=CONF_HELP)
@click.option("-d", "--debug/--no-debug", default=False, help="Enable debug mode")
@click.option("-p", "--pipeline", default=None, help="Bundle a single pipeline")
@click.option(
    "-v",
    "--var",
    default=[],
    help='set values for variables defined in bundle config. Example: --var="foo=bar"',
    multiple=True,
)
@click.pass_obj
def deploy(
    metadata: ProjectMetadata,
    env: str,
    target: str | None,
    bundle: bool,
    conf: str,
    pipeline: str,
    debug: bool,
    var: list[str],
):
    """Deploy the asset bundle to Databricks"""

    if shutil.which("databricks") is None:  # pragma: no cover
        raise Exception("databricks CLI is not installed")
    controller = DeployController(metadata)
    controller.go_to_project()
    controller.validate_databricks_config()
    controller.build_project()
    if bundle is True:
        bundle_controller = BundleController(metadata, env, conf)
        workflows = bundle_controller.generate_resources(pipeline)
        bundle_resources = bundle_controller.apply_overrides(workflows, "default")
        bundle_controller.save_bundled_resources(bundle_resources, overwrite=True)
    controller.create_dbfs_dir()
    controller.upload_project_config(conf)
    controller.upload_project_data()
    if target is None:
        target = env
    controller.deploy_project(target=target, debug=debug, var=var)
