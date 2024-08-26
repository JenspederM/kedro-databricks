from __future__ import annotations

import logging
import shutil
from typing import Any

import click
from kedro.config import AbstractConfigLoader, MissingConfigException
from kedro.framework.cli.utils import ENV_HELP
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.bundle import (
    apply_resource_overrides,
    generate_resources,
    save_bundled_resources,
)
from kedro_databricks.deploy import (
    build_project,
    create_dbfs_dir,
    deploy_project,
    go_to_project,
    upload_project_config,
    upload_project_data,
    validate_databricks_config,
)
from kedro_databricks.init import (
    substitute_catalog_paths,
    write_bundle_template,
    write_databricks_run_script,
    write_override_template,
)

DEFAULT_RUN_ENV = "local"
DEFAULT_CONFIG_KEY = "default"
DEFAULT_CONFIG_HELP = "Set the key for the default configuration"
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


def _load_config(
    config_loader: AbstractConfigLoader, package_name: str
) -> dict[str, Any]:
    log = logging.getLogger(package_name)
    # Backwards compatibility for ConfigLoader that does not support `config_patterns`
    if not hasattr(config_loader, "config_patterns"):
        return config_loader.get("databricks*", "databricks/**")  # pragma: no cover

    # Set the default pattern for `databricks` if not provided in `settings.py`
    if "databricks" not in config_loader.config_patterns.keys():
        config_loader.config_patterns.update(  # pragma: no cover
            {"databricks": ["databricks*", "databricks/**"]}
        )

    assert "databricks" in config_loader.config_patterns.keys()

    # Load the config
    try:
        return config_loader["databricks"]
    except MissingConfigException:  # pragma: no cover
        log.warning("No Databricks configuration found.")
        return {}


def _load_env_config(metadata: ProjectMetadata, env: str, MSG: str):
    log = logging.getLogger(metadata.package_name)
    # If the configuration directory does not exist, Kedro will not load any configuration
    conf_dir = metadata.project_path / "conf" / env
    if not conf_dir.exists():
        log.warning(f"{MSG}: Creating {conf_dir.relative_to(metadata.project_path)}")
        conf_dir.mkdir(parents=True)

    with KedroSession.create(project_path=metadata.project_path, env=env) as session:
        return _load_config(
            config_loader=session._get_config_loader(),
            package_name=session._package_name,
        )


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
    write_bundle_template(metadata)
    write_override_template(metadata, default, _PROVIDER_MAP.get(provider))
    write_databricks_run_script(metadata)
    substitute_catalog_paths(metadata)


@databricks_commands.command()
@click.option("-d", "--default", default=DEFAULT_CONFIG_KEY, help=DEFAULT_CONFIG_HELP)
@click.option("-e", "--env", default=DEFAULT_RUN_ENV, help=ENV_HELP)
@click.option("--overwrite", default=False, help="Overwrite the existing resources")
@click.pass_obj
def bundle(
    metadata: ProjectMetadata,
    default: str,
    env: str,
    overwrite: bool,
):
    """Convert kedro pipelines into Databricks asset bundle resources"""
    if default.startswith("_"):
        raise ValueError(
            "Default key cannot start with `_` as this is not recognized by OmegaConf."
        )

    MSG = "Create Asset Bundle Resources"
    overrides = _load_env_config(metadata, env, MSG)
    workflows = generate_resources(pipelines, metadata, env, MSG)
    bundle_resources = apply_resource_overrides(workflows, overrides, default)
    save_bundled_resources(bundle_resources, metadata, overwrite)


@databricks_commands.command()
@click.option("-e", "--env", default=DEFAULT_RUN_ENV, help=ENV_HELP)
@click.option("-t", "--target", default=None, help="Databricks target environment. Defaults to the `env` value.")
@click.option(
    "-b",
    "--bundle/--no-bundle",
    default=False,
    help="Bundle the project before deploying",
)
@click.option("-d", "--debug/--no-debug", default=False, help="Enable debug mode")
@click.pass_obj
def deploy(
    metadata: ProjectMetadata,
    env: str,
    target: str | None,
    bundle: bool,
    debug: bool,
):
    """Deploy the asset bundle to Databricks"""
    MSG = "Deploying to Databricks"
    if shutil.which("databricks") is None:  # pragma: no cover
        raise Exception("databricks CLI is not installed")
    go_to_project(metadata)
    validate_databricks_config(metadata)
    build_project(metadata, MSG=MSG)
    if bundle is True:
        overrides = _load_env_config(metadata, env, MSG)
        workflows = generate_resources(pipelines, metadata, env, MSG)
        bundle_resources = apply_resource_overrides(workflows, overrides, "default")
        save_bundled_resources(bundle_resources, metadata, True)
    create_dbfs_dir(metadata, MSG=MSG)
    upload_project_config(metadata, MSG=MSG)
    upload_project_data(metadata, MSG=MSG)
    if target is None:
        target = env
    deploy_project(metadata, MSG=MSG, target=target, debug=debug)
