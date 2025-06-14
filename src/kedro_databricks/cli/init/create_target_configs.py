import json
import re
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.constants import DEFAULT_TARGET
from kedro_databricks.logger import get_logger
from kedro_databricks.utils import (
    Command,
    get_bundle_name,
    get_targets,
    read_databricks_config,
)

log = get_logger("init")


class DatabricksTarget:
    """Represents a Databricks target for a Kedro project.

    This class is used to create a target configuration for a Databricks Asset Bundle.
    It retrieves metadata about the target from the Databricks CLI and stores
    relevant information such as the bundle name, target name, mode, host, and file path.

    Attributes:
        bundle (str): The name of the Databricks bundle.
        name (str): The name of the target.
        mode (str): The mode of the target (e.g., "development").
        host (str): The host of the Databricks workspace.
        file_path (str): The file path in the Databricks workspace.

    Args:
        bundle (str): The name of the Databricks bundle.
        name (str): The name of the target.
        conf (dict): The configuration dictionary for the target, which may include
            mode and workspace information.

    Raises:
        ValueError: If the metadata for the target cannot be retrieved.
    """

    def __init__(self, bundle: str, name: str, conf: dict):
        self.bundle = bundle
        self.name = name
        self.mode = conf.get("mode", "development")
        workspace_conf = conf.get("workspace", {})
        self.host = workspace_conf.get("host")
        metadata = self._get_metadata()
        self.file_path = metadata.get("workspace", {}).get("file_path")

    def _get_metadata(self):
        result = Command(
            [
                "databricks",
                "bundle",
                "validate",
                "--target",
                self.name,
                "--output",
                "json",
            ],
            log=log,
            warn=True,
        ).run()
        json_start = [
            i for i in range(len(result.stdout)) if result.stdout[i].startswith("{")
        ]
        if not json_start:  # pragma: no cover
            raise ValueError(f"Could not get metadata for target {self.name}")
        json_output = "\n".join(result.stdout[json_start[0] :])
        return json.loads(json_output)


def create_target_configs(
    metadata: ProjectMetadata,
    node_type_id: str,
    default_key: str,
    single_user_default: bool = True,
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
        node_type_id (str): The node type ID for the target configuration.
        default_key (str): The default key to use for the target configuration.
        single_user_default (bool, optional): Whether to set the target as single user by default.
            Defaults to True.

    Raises:
        FileNotFoundError: If the Databricks configuration file does not exist.
        ValueError: If the Databricks configuration is invalid or missing required fields.
    """
    conf_dir = metadata.project_path / "conf"
    databricks_config = read_databricks_config(metadata.project_path)
    bundle_name = get_bundle_name(databricks_config)
    targets = get_targets(databricks_config)
    for target_name, target_conf in targets.items():
        target = DatabricksTarget(bundle_name, target_name, target_conf)
        target_conf_dir = conf_dir / target.name
        target_conf_dir.mkdir(exist_ok=True)
        _save_gitkeep_file(target_conf_dir)
        is_single_user = single_user_default and target.name == DEFAULT_TARGET
        target_config = _create_target_config(
            default_key,
            node_type_id,
            single_user=is_single_user,
        )
        _save_target_config(target_config, target_conf_dir)
        target_file_path = f"/Volumes/<your-volume-name>/{bundle_name}/{target_name}"
        if target.name == DEFAULT_TARGET:
            target_file_path = f"/dbfs/FileStore/{bundle_name}/{target_name}"
        _save_target_catalog(conf_dir, target_conf_dir, target_file_path)
        log.info(f"Created target config for {target.name} at {target_conf_dir}")


def _create_target_config(
    default_key: str, node_type_id: str, single_user: bool = False
):
    new_cluster = {
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": node_type_id,
        "num_workers": 1,
        "spark_env_vars": {
            "KEDRO_LOGGING_CONFIG": "/Workspace/\\${workspace.file_path}/conf/logging.yml"
        },
    }

    if single_user:
        wc = WorkspaceClient()
        single_user_opts = {
            "data_security_mode": "SINGLE_USER",
            "single_user_name": wc.current_user.me().user_name,
        }
        new_cluster.update(single_user_opts)

    return {
        default_key: {
            "job_clusters": [
                {"job_cluster_key": default_key, "new_cluster": new_cluster}
            ],
            "tasks": [{"task_key": default_key, "job_cluster_key": default_key}],
        }
    }


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
