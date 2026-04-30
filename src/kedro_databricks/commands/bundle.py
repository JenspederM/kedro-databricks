import copy
from pathlib import Path
from typing import Any

import click
import yaml
from kedro.config import MissingConfigException, OmegaConfigLoader
from kedro.framework.session import KedroSession
from kedro.framework.startup import ProjectMetadata

import kedro_databricks.commands._options as option
from kedro_databricks.config import config
from kedro_databricks.resource_generator import (
    RESOURCE_GENERATOR_RESOLVER,
)
from kedro_databricks.resource_overrider import RESOURCE_OVERRIDER_RESOLVER
from kedro_databricks.utilities.logger import get_logger

log = get_logger("bundle")


class NoOverridesError(Exception):
    def __init__(self, conf_source: Path) -> None:
        msg = f"Could not find any override definitions in {conf_source}"
        super().__init__(msg)


class NoResourcesKeyError(Exception):
    def __init__(self, env: str) -> None:
        msg = f"'resources' key not found in the 'databricks' configuration for environment '{env}'."
        super().__init__(msg)


@click.command()
@option.default_key
@option.overwrite
@option.resource_generator
@option.env
@option.conf_source
@option.pipeline
@option.params
@click.pass_obj
def command(
    metadata: ProjectMetadata,
    default_key: str,
    overwrite: bool,
    resource_generator: str,
    env: str,
    conf_source: str,
    pipeline: str | None,
    params: str | None,
):
    """Databricks Asset Bundle commands"""
    # If the configuration directory does not exist, Kedro will not load any configuration
    local_config_dir = metadata.project_path / conf_source / env
    if not local_config_dir.exists():
        log.warning(f"Creating {local_config_dir.relative_to(metadata.project_path)}")
        local_config_dir.mkdir(parents=True)

    with KedroSession.create(project_path=metadata.project_path, env=env) as session:
        overrides = _load_kedro_env_config(session=session)
        if not overrides:
            raise NoOverridesError(Path(conf_source))
        elif "resources" not in overrides:
            raise NoResourcesKeyError(env)

        ResourceGenerator = RESOURCE_GENERATOR_RESOLVER.resolve(resource_generator)

        g = ResourceGenerator(
            session=session,
            metadata=metadata,
            conf_source=conf_source,
            params=params,
        )

        all_resources = {"jobs": g.generate_jobs(pipeline)}
        overridden_resources = {}
        for resource_type, resource_override_items in overrides["resources"].items():
            overridden_resources[resource_type] = {}
            resource_items = all_resources.get(resource_type, {})
            overrider = RESOURCE_OVERRIDER_RESOLVER.resolve(resource_type)()
            all_keys = set(resource_items.keys()).union(
                set(resource_override_items.keys())
            )
            for key in all_keys:
                if key == default_key or key.startswith(config.regex_prefix):
                    continue
                resource = resource_items.get(key, {})
                overridden_resources[resource_type][key] = overrider.override(
                    resource_key=key,
                    resource=resource,
                    overrides=copy.deepcopy(resource_override_items),
                    default_key=default_key,
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


def _load_kedro_env_config(session: KedroSession) -> dict[str, Any]:
    """Load the Databricks configuration for the given environment.

    Args:
        metadata (ProjectMetadata): The metadata of the project
        config_dir (str): The name of the configuration directory
        env (str): The name of the kedro environment

    Returns:
        dict[str, Any]: The Databricks configuration for the given environment
    """
    config_loader = session._get_config_loader()
    # Backwards compatibility for ConfigLoader that does not support `config_patterns`
    if not hasattr(config_loader, "config_patterns"):  # pragma: no cover
        return config_loader.get("databricks*", "databricks/**")

    if not isinstance(config_loader, OmegaConfigLoader):  # pragma: no cover
        raise TypeError(
            "Only OmegaConfigLoader is supported to load Databricks configuration."
        )

    # Set the default pattern for `databricks` if not provided in `settings.py`
    if "databricks" not in config_loader.config_patterns.keys():  # pragma: no cover
        config_loader.config_patterns.update(
            {"databricks": ["databricks*", "databricks/**"]}
        )

    if "databricks" not in config_loader.config_patterns.keys():  # pragma: no cover
        log.warning(
            "No Databricks configuration found. "
            "Please ensure that `databricks` is included in your config patterns."
        )

    try:
        return config_loader["databricks"]
    except MissingConfigException:
        log.warning("No Databricks configuration found.")
        return {}
