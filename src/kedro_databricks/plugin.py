import logging
from typing import Any

import click
import yaml
from kedro.config import AbstractConfigLoader, MissingConfigException
from kedro.framework.cli.utils import ENV_HELP
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.bundle import apply_resource_overrides, generate_resources
from kedro_databricks.deploy import deploy_to_databricks
from kedro_databricks.init import (
    substitute_catalog_paths,
    write_bundle_template,
    write_databricks_run_script,
    write_override_template,
)

DEFAULT_RUN_ENV = "local"
DEFAULT_CONFIG_KEY = "default"
DEFAULT_CONFIG_HELP = "Set the key for the default configuration"


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
    except MissingConfigException:
        log.warning("No Databricks configuration found.")
        return {}


@databricks_commands.command()
@click.option("-d", "--default", default=DEFAULT_CONFIG_KEY, help=DEFAULT_CONFIG_HELP)
@click.pass_obj
def init(
    metadata: ProjectMetadata,
    default: str,
):
    """Initialize Databricks Asset Bundle configuration"""
    write_bundle_template(metadata)
    write_override_template(metadata, default)
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
    MSG = "Create Asset Bundle Resources"
    package_name = metadata.package_name
    project_path = metadata.project_path
    log = logging.getLogger(package_name)

    # If the configuration directory does not exist, Kedro will not load any configuration
    conf_dir = metadata.project_path / "conf" / env
    if not conf_dir.exists():
        log.warning(f"{MSG}: Creating {conf_dir.relative_to(project_path)}")
        conf_dir.mkdir(parents=True)

    with KedroSession.create(project_path=metadata.project_path, env=env) as session:
        resource_overrides = _load_config(
            config_loader=session._get_config_loader(),
            package_name=session._package_name,
        )

    if default.startswith("_"):
        raise ValueError(
            "Default key cannot start with `_` as this is not recognized by OmegaConf."
        )

    pipeline_resources = generate_resources(pipelines, metadata)

    bundle_resources = apply_resource_overrides(
        pipeline_resources,
        resource_overrides,
        default_key=default,
    )

    resources_dir = project_path / "resources"
    resources_dir.mkdir(exist_ok=True)

    for name, resource in bundle_resources.items():
        MSG = f"Writing resource '{name}'"
        p = resources_dir / f"{name}.yml"

        if p.exists() and not overwrite:  # pragma: no cover
            log.warning(
                f"{MSG}: {p.relative_to(project_path)} already exists."
                " Use --overwrite to replace."
            )
            continue

        with open(p, "w") as f:
            log.info(f"{MSG}: Wrote {p.relative_to(project_path)}")
            yaml.dump(resource, f, default_flow_style=False, indent=4, sort_keys=False)


@databricks_commands.command()
@click.option("-e", "--env", default=DEFAULT_RUN_ENV, help=ENV_HELP)
@click.option(
    "-b",
    "--bundle/--no-bundle",
    default=False,
    help="Bundle the project before deploying",
)
@click.option("-b", "--debug/--no-debug", default=False, help="Enable debug mode")
@click.pass_obj
def deploy(
    metadata: ProjectMetadata,
    env: str,
    bundle: bool,
    debug: bool,
):
    """Deploy the asset bundle to Databricks"""
    # Load context to initialize logging
    deploy_to_databricks(metadata, env, bundle, debug=debug)
