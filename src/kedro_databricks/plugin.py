import logging
from typing import Any
import click
import yaml

from kedro.config import MissingConfigException
from kedro.framework.cli.utils import ENV_HELP
from kedro.framework.context import KedroContext
from kedro.framework.session import KedroSession
from kedro.framework.project import pipelines, configure_project
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.bundle import generate_resources, apply_resource_overrides
from kedro_databricks.init import create_databricks_config, write_default_config

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


def _load_config(context: KedroContext) -> dict[str, Any]:
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
        # File does not exist
        return {}


@databricks_commands.command()
@click.option("-d", "--default", default=DEFAULT_CONFIG_KEY, help=DEFAULT_CONFIG_HELP)
@click.pass_obj
def init(
    metadata: ProjectMetadata,
    default: str,
):
    """Initialize the Databricks bundle"""
    path = metadata.project_path
    conf_path = path / "conf" / "base" / "databricks.yml"
    create_databricks_config(path, metadata.package_name)
    write_default_config(conf_path, default, metadata.package_name)


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
    """Bundle the pipeline for Databricks"""
    log = logging.getLogger(metadata.package_name)
    pipeline_resources = generate_resources(pipelines, metadata.package_name)

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
        package_name=metadata.package_name,
    )

    resources_dir = metadata.project_path / "resources"
    resources_dir.mkdir(exist_ok=True)

    for name, resource in bundle_resources.items():
        p = resources_dir / f"{name}.yml"

        if p.exists() and not overwrite:
            raise KeyError(f"Resource '{name}' already exists. Skipping.")

        with open(p, "w") as f:
            log.info(f"Writing resource '{name}'")
            yaml.dump(resource, f, default_flow_style=False, indent=4, sort_keys=False)


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
    """Initialize the Databricks bundle"""
    logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
    logging.getLogger("py4j.py4j.clientserver").setLevel(logging.ERROR)

    configure_project(metadata.package_name)
    with KedroSession.create(env=env, conf_source=conf_source) as session:
        session.run(node_names=nodes)
