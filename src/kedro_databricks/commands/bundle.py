from typing import Any

import click
import yaml
from kedro.config import MissingConfigException, OmegaConfigLoader
from kedro.framework.cli.project import (
    CONF_SOURCE_HELP,
    PARAMS_ARG_HELP,
    PIPELINE_ARG_HELP,
)
from kedro.framework.cli.utils import ENV_HELP
from kedro.framework.session import KedroSession
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.constants import (
    DEFAULT_CONF_FOLDER,
    DEFAULT_CONFIG_GENERATOR,
    DEFAULT_CONFIG_GENERATOR_HELP,
    DEFAULT_CONFIG_KEY,
    DEFAULT_CONFIG_KEY_HELP,
    DEFAULT_ENV,
)
from kedro_databricks.utilities.logger import get_logger
from kedro_databricks.utilities.resource_generator import RESOURCE_GENERATOR_RESOLVER
from kedro_databricks.utilities.resource_overrider import RESOURCE_OVERRIDER_RESOLVER

log = get_logger("bundle")


@click.command()
@click.option(
    "-d",
    "--default-key",
    default=DEFAULT_CONFIG_KEY,
    help=DEFAULT_CONFIG_KEY_HELP,
)
@click.option(
    "-g",
    "--resource-generator",
    default=DEFAULT_CONFIG_GENERATOR,
    help=DEFAULT_CONFIG_GENERATOR_HELP,
)
@click.option(
    "-e",
    "--env",
    default=DEFAULT_ENV,
    help=ENV_HELP,
)
@click.option(
    "-c",
    "--conf-source",
    default=DEFAULT_CONF_FOLDER,
    help=CONF_SOURCE_HELP,
)
@click.option(
    "-p",
    "--pipeline",
    default=None,
    help=PIPELINE_ARG_HELP,
)
@click.option(
    "-r",
    "--params",
    default=None,
    help=PARAMS_ARG_HELP,
)
@click.option(
    "--overwrite",
    default=False,
    is_flag=True,
    show_default=True,
    help="Overwrite the existing resources",
)
@click.pass_obj
def command(
    metadata: ProjectMetadata,
    default_key: str,
    resource_generator: str,
    env: str,
    conf_source: str,
    pipeline: str | None,
    params: str | None,
    overwrite: bool,
):
    """Databricks Asset Bundle commands"""
    if default_key.startswith("_"):  # pragma: no cover
        raise ValueError(
            "Default key cannot start with `_` as this is not recognized by OmegaConf."
        )

    ResourceGenerator = RESOURCE_GENERATOR_RESOLVER.resolve(resource_generator)

    if not (metadata.project_path / conf_source / env / "databricks.yml").exists():
        raise FileNotFoundError(
            f"Databricks configuration for environment '{env}' not found "
            f"in '{conf_source}/{env}/databricks.yml'."
        )

    overrides = _load_kedro_env_config(metadata, config_dir=conf_source, env=env)
    if "resources" not in overrides:
        raise KeyError(
            f"'resources' key not found in the 'databricks' configuration for environment '{env}'."
        )

    g = ResourceGenerator(metadata, env, conf_source, params)
    all_resources = {"jobs": g.generate_jobs(pipeline)}
    overridden_resources = {}
    for resource_type, resource_override_items in overrides["resources"].items():
        overridden_resources[resource_type] = {}

        resource_items = all_resources.get(resource_type, {})
        default_overrides = resource_override_items.pop(default_key, {})

        overrider = RESOURCE_OVERRIDER_RESOLVER.resolve(resource_type)()
        all_keys = set(resource_items.keys()).union(set(resource_override_items.keys()))
        for key in all_keys:
            resource = resource_items.get(key, {})
            resource_overrides = resource_override_items.get(key, {})
            overridden_resources[resource_type][key] = overrider.override(
                resource_key=key,
                default_key=default_key,
                resource=resource,
                overrides={
                    default_key: default_overrides,
                    key: resource_overrides,
                },
            )
    save_resources(
        metadata=metadata,
        env=env,
        resources=overridden_resources,
        overwrite=overwrite,
    )


def save_resources(
    metadata: ProjectMetadata,
    env: str,
    resources: dict[str, dict[str, Any]],
    overwrite: bool,
) -> None:
    """Save the given resources to the project directory.

    Args:
        metadata (ProjectMetadata): The metadata of the project
        env (str): The kedro environment
        resources (dict[str, dict[str, Any]]): The resources to save
        overwrite (bool): Whether to overwrite existing resources
    """
    resources_dir = metadata.project_path / "resources"
    resources_dir.mkdir(exist_ok=True, parents=True)
    for resource_type, items in resources.items():
        for resource_name, resource in items.items():
            file_name = f"target.{env}.{resource_type}.{resource_name}"
            file_path = resources_dir / f"{file_name}.yml"
            relative_path = file_path.relative_to(metadata.project_path)

            if file_path.exists() and not overwrite:  # pragma: no cover
                log.warning(
                    f"{relative_path} already exists. Use --overwrite to replace."
                )
                continue

            with open(file_path, "w") as f:
                yaml.dump(
                    {
                        "targets": {
                            env: {
                                "resources": {resource_type: {resource_name: resource}}
                            }
                        }
                    },
                    f,
                    default_flow_style=False,
                    indent=4,
                    sort_keys=False,
                )
                if file_path.exists() and overwrite:
                    log.info(f"Overwrote {relative_path}")
                else:
                    log.info(f"Wrote {relative_path}")


def _load_kedro_env_config(
    metadata: ProjectMetadata, config_dir: str, env: str
) -> dict[str, Any]:
    """Load the Databricks configuration for the given environment.

    Args:
        metadata (ProjectMetadata): The metadata of the project
        config_dir (str): The name of the configuration directory
        env (str): The name of the kedro environment

    Returns:
        dict[str, Any]: The Databricks configuration for the given environment
    """
    local_config_dir = metadata.project_path / config_dir / env

    # If the configuration directory does not exist, Kedro will not load any configuration
    if not local_config_dir.exists():
        log.warning(f"Creating {local_config_dir.relative_to(metadata.project_path)}")
        local_config_dir.mkdir(parents=True)

    with KedroSession.create(project_path=metadata.project_path, env=env) as session:
        config_loader = session._get_config_loader()
        # Backwards compatibility for ConfigLoader that does not support `config_patterns`
        if not hasattr(config_loader, "config_patterns"):
            return config_loader.get("databricks*", "databricks/**")

        if not isinstance(config_loader, OmegaConfigLoader):
            raise TypeError(
                "Only OmegaConfigLoader is supported to load Databricks configuration."
            )

        # Set the default pattern for `databricks` if not provided in `settings.py`
        if "databricks" not in config_loader.config_patterns.keys():
            config_loader.config_patterns.update(
                {"databricks": ["databricks*", "databricks/**"]}
            )

        if "databricks" not in config_loader.config_patterns.keys():
            log.warning(
                "No Databricks configuration found. "
                "Please ensure that `databricks` is included in your config patterns."
            )

        try:
            return config_loader["databricks"]
        except MissingConfigException:
            log.warning("No Databricks configuration found.")
            return {}
