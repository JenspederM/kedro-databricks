"""This module provides functionality to bundle Kedro pipelines into Databricks asset bundle resources."""

from __future__ import annotations

from typing import Any

import yaml
from kedro.config import MissingConfigException
from kedro.framework.session import KedroSession
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.cli.bundle.generate_resources import ResourceGenerator
from kedro_databricks.cli.bundle.override_resources import override_resources
from kedro_databricks.logger import get_logger

log = get_logger("bundle")


def bundle(
    metadata: ProjectMetadata,
    env: str,
    default_key: str,
    conf_source: str = "conf",
    params: str | None = None,
    pipeline_name: str | None = None,
    overwrite: bool = False,
):
    """Convert Kedro pipelines into Databricks asset bundle resources.

    This function generates asset bundle resources for the specified Kedro pipeline
    and saves them in the `resources` directory. To validate the generated resources,
    you can run the `databricks bundle validate` command.

    Args:
        metadata (ProjectMetadata): The metadata of the project.
        env (str): The environment for the Kedro project (e.g., "dev", "prod").
        default_key (str): The default configuration key to use for Databricks.
        conf_source (str): The source of the Kedro configuration files (default: "conf").
        params (str | None): Kedro run time parameters in `key1=value1,key2=value2` format (optional).
        pipeline_name (str | None): The pipeline to bundle (optional).
        overwrite (bool): Whether to overwrite existing resources (default: False).
    """
    if default_key.startswith("_"):  # pragma: no cover
        raise ValueError(
            "Default key cannot start with `_` as this is not recognized by OmegaConf."
        )
    overrides = _load_kedro_env_config(metadata, config_dir=conf_source, env=env)
    g = ResourceGenerator(metadata, env, conf_source, params)
    resources = g.generate_resources(pipeline_name)
    result = {}
    for name, resource in resources.items():
        result[name] = override_resources(resource, overrides, default_key)
    _save_bundled_resources(metadata, result, overwrite)


def _load_kedro_env_config(
    metadata: ProjectMetadata, config_dir: str, env: str
) -> dict[str, Any]:
    """Load the Databricks configuration for the given environment.

    Args:
        metadata (ProjectMetadata): The metadata of the project
        config_dir (str): The name of the configuration directory
        env (str): The name of the kedro environment
        MSG (str): The message to display

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
            return config_loader.get("databricks*", "databricks/**")  # pragma: no cover

        # Set the default pattern for `databricks` if not provided in `settings.py`
        if "databricks" not in config_loader.config_patterns.keys():  # type: ignore
            config_loader.config_patterns.update(  # pragma: no cover # type: ignore
                {"databricks": ["databricks*", "databricks/**"]}
            )

        if "databricks" not in config_loader.config_patterns.keys():  # type: ignore
            log.warning(
                "No Databricks configuration found. "
                "Please ensure that `databricks` is included in your config patterns."
            )

        # Load the config
        try:
            return config_loader["databricks"]
        except MissingConfigException:  # pragma: no cover
            log.warning("No Databricks configuration found.")
            return {}


def _save_bundled_resources(
    metadata: ProjectMetadata,
    resources: dict[str, dict[str, Any]],
    overwrite: bool = False,
):
    """Save the generated resources to the project directory.

    Args:
        resources (Dict[str, Dict[str, Any]]): A dictionary of pipeline names and their Databricks resources
        metadata (ProjectMetadata): The metadata of the project
        overwrite (bool): Whether to overwrite existing resources
    """
    resources_dir = metadata.project_path / "resources"
    resources_dir.mkdir(exist_ok=True, parents=True)
    for name, resource in resources.items():
        p = resources_dir / f"{name}.yml"
        relative_path = p.relative_to(metadata.project_path)

        if p.exists() and not overwrite:  # pragma: no cover
            log.warning(f"{relative_path} already exists. Use --overwrite to replace.")
            continue

        with open(p, "w") as f:
            yaml.dump(resource, f, default_flow_style=False, indent=4, sort_keys=False)
            log.info(f"Wrote {relative_path}")
