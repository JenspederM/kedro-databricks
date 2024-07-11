import logging
from typing import Any

import click
import yaml
from kedro.config import MissingConfigException
from kedro.framework.cli.utils import ENV_HELP
from kedro.framework.context import KedroContext
from kedro.framework.project import configure_project, pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import ProjectMetadata

from kedro_databricks import LOGGING_NAME
from kedro_databricks.bundle import apply_resource_overrides, generate_resources
from kedro_databricks.deploy import deploy_to_databricks
from kedro_databricks.init import create_databricks_config, write_default_config

DEFAULT_RUN_ENV = "dev"
DEFAULT_CONFIG_KEY = "default"
DEFAULT_CONFIG_HELP = "Set the key for the default configuration"


@click.group(name="Kedro-Databricks")
def commands():
    pass


@commands.group(name="databricks")
def databricks_commands():
    """Run project with Databricks"""
    pass


def _load_config(context: KedroContext) -> dict[str, Any]:
    log = logging.getLogger(LOGGING_NAME)
    # Backwards compatibility for ConfigLoader that does not support `config_patterns`
    config_loader = context.config_loader
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

    # Load context to initialize logging
    with KedroSession.create(project_path=metadata.project_path) as session:
        session.load_context()

    create_databricks_config(metadata)
    write_default_config(metadata, default)


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
    log = logging.getLogger(LOGGING_NAME)
    pipeline_resources = generate_resources(pipelines, metadata.package_name)

    # If the configuration directory does not exist, Kedro will not load any configuration
    conf_dir = metadata.project_path / "conf" / env
    if not conf_dir.exists():
        conf_dir.mkdir(parents=True)

    with KedroSession.create(project_path=metadata.project_path, env=env) as session:
        context = session.load_context()
        resource_overrides = _load_config(context)

    if default.startswith("_"):
        raise ValueError(
            "Default key cannot start with `_` as this is not recognized by OmegaConf."
        )

    bundle_resources = apply_resource_overrides(
        pipeline_resources,
        resource_overrides,
        default_key=default,
    )

    resources_dir = metadata.project_path / "resources"
    resources_dir.mkdir(exist_ok=True)

    for name, resource in bundle_resources.items():
        p = resources_dir / f"{name}.yml"

        if p.exists() and not overwrite:  # pragma: no cover
            log.warning(f"Resource '{name}' already exists. Skipping.")
            continue

        with open(p, "w") as f:
            log.info(f"Writing resource '{name}'")
            yaml.dump(resource, f, default_flow_style=False, indent=4, sort_keys=False)


@databricks_commands.command()
@click.option("-e", "--env", default=DEFAULT_RUN_ENV, help=ENV_HELP)
@click.option("-b", "--bundle/--no-bundle", default=False, help=ENV_HELP)
@click.pass_obj
def deploy(
    metadata: ProjectMetadata,
    env: str,
    bundle: bool,
):
    """Deploy the asset bundle to Databricks"""
    # Load context to initialize logging
    deploy_to_databricks(metadata, env, bundle)


@databricks_commands.command()
@click.option("-e", "--env", default=DEFAULT_RUN_ENV, help=ENV_HELP)
@click.option("-c", "--conf-source")
@click.option("-n", "--nodes")
@click.pass_obj
def run(
    metadata: ProjectMetadata,
    env: str,
    conf_source: str,
    nodes: str,
):
    """Run the project on Databricks"""
    logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
    logging.getLogger("py4j.py4j.clientserver").setLevel(logging.ERROR)

    configure_project(metadata.package_name)
    with KedroSession.create(env=env, conf_source=conf_source) as session:
        session.run(node_names=[node.strip() for node in nodes.split(",")])
