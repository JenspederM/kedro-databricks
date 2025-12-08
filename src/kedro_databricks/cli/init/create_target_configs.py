import re
from pathlib import Path

import yaml
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.core.logger import get_logger

log = get_logger("init")


def create_target_configs(
    metadata: ProjectMetadata,
    default_key: str,
    default_catalog: str,
    default_schema: str,
):
    """Create target configurations for a Kedro project in Databricks.

    This function creates target configurations for each target defined in the
    Databricks configuration file. It sets up the necessary directories and files
    for each target, including a `.gitkeep` file to ensure the directory is tracked
    by Git. It also creates a target configuration file with the specified node type
    and default key. If the target is the default target, it sets up a specific file path
    for it in the Databricks File System (DBFS).


    Args:
        metadata (ProjectMetadata): The project metadata containing the project path.
        default_key (str): The default key to use for the target configuration.
        default_catalog (str): The default catalog to use for the Databricks target.
        default_schema (str): The default schema to use for the Databricks target.

    Raises:
        FileNotFoundError: If the Databricks configuration file does not exist.
        ValueError: If the Databricks configuration is invalid or missing required fields.
    """
    conf_dir = metadata.project_path / "conf"
    databricks_config = _read_databricks_config(metadata.project_path)
    bundle_name = _get_bundle_name(databricks_config)
    targets = _get_targets(databricks_config)
    for target_name in targets.keys():
        target_conf_dir = conf_dir / target_name
        target_conf_dir.mkdir(exist_ok=True)
        _save_gitkeep_file(target_conf_dir)
        target_config = _create_target_config(default_key, bundle_name)
        _save_target_config(target_config, target_conf_dir)
        target_file_path = make_target_file_path(
            default_catalog,
            default_schema,
            bundle_name,
            target_name,
        )
        _save_target_catalog(conf_dir, target_conf_dir, target_file_path)
        log.info(f"Created target config for {target_name} at {target_conf_dir}")


def _create_target_config(default_key: str, bundle_name: str):
    return {
        "resources": {
            "volumes": {
                f"{bundle_name}_volume": {
                    "catalog_name": "workspace",
                    "schema_name": "default",
                    "name": bundle_name,
                    "comment": f"Volume for {bundle_name}",
                    "volume_type": "MANAGED",
                    "grants": [
                        {
                            "principal": "\\${workspace.current_user.userName}",
                            "privileges": ["READ_VOLUME", "WRITE_VOLUME"],
                        },
                    ],
                }
            },
            "jobs": {
                default_key: {
                    "environments": [
                        {
                            "environment_key": default_key,
                            "spec": {
                                "environment_version": "4",
                                "dependencies": ["../dist/*.whl"],
                            },
                        }
                    ],
                    "tasks": [
                        {
                            "task_key": default_key,
                            "environment_key": default_key,
                        }
                    ],
                }
            },
        }
    }


def make_target_file_path(
    catalog_name: str,
    schema_name: str,
    bundle_name: str,
    target_name: str,
) -> str:
    """Create the file path for the Databricks target.

    Args:
        catalog_name (str): The name of the catalog.
        schema_name (str): The name of the schema.
        bundle_name (str): The name of the Databricks bundle.
        target_name (str): The name of the target.

    Returns:
        str: The file path for the target in Databricks.
    """
    return f"/Volumes/{catalog_name}/{schema_name}/{bundle_name}/{target_name}"


def _substitute_file_path(string: str) -> str:
    """Substitute the file path in the catalog"""
    match = re.sub(
        r"(.*:)(.*)(data/.*)",
        r"\g<1> ${_file_path}/\g<3>",
        string,
    )
    return match


def _save_target_catalog(
    conf_dir: Path, target_conf_dir: Path, target_file_path: str
):  # pragma: no cover
    with open(f"{conf_dir}/base/catalog.yml") as f:
        cat = f.read()
    target_catalog = _substitute_file_path(cat)
    with open(target_conf_dir / "catalog.yml", "w") as f:
        f.write("_file_path: " + target_file_path + "\n" + target_catalog)


def _save_target_config(target_config: dict, target_conf_dir: Path):  # pragma: no cover
    with open(target_conf_dir / "databricks.yml", "w") as f:
        yaml.dump(target_config, f)


def _save_gitkeep_file(target_conf_dir: Path):
    if not (target_conf_dir / ".gitkeep").exists():
        with open(target_conf_dir / ".gitkeep", "w") as f:
            f.write("")


def _read_databricks_config(project_path: Path) -> dict:
    """Read the databricks.yml configuration file.

    Args:
        project_path (Path): The path to the Kedro project.

    Returns:
        dict: The configuration as a dictionary.
    """
    with open(project_path / "databricks.yml") as f:
        conf = yaml.safe_load(f)
    return conf


def _get_bundle_name(config: dict) -> str:
    """Get the bundle name from the databricks.yml configuration.

    Args:
        config (dict): The configuration as a dictionary.

    Returns:
        str: The bundle name.

    Raises:
        ValueError: If the bundle name is not found.
    """
    bundle_name = config.get("bundle", {}).get("name")
    if bundle_name is None:
        raise ValueError("No `bundle.name` found in databricks.yml")
    return bundle_name


def _get_targets(config: dict) -> dict:
    """Get the targets from the databricks.yml configuration.

    Args:
        config (dict): The configuration as a dictionary.

    Returns:
        dict: The targets as a dictionary.

    Raises:
        ValueError: If the targets are not found.
    """
    targets = config.get("targets")
    if targets is None:
        raise ValueError("No `targets` found in databricks.yml")
    return targets
