import json
import logging
import re
from pathlib import Path

import yaml
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.utils.common import Command

log = logging.getLogger(__name__)


def create_target_configs(
    metadata: ProjectMetadata, node_type_id: str, default_key: str
):
    kedro_databricks_config = {
        default_key: {
            "job_clusters": [
                {
                    "job_cluster_key": default_key,
                    "new_cluster": {
                        "spark_version": "15.4.x-scala2.12",
                        "node_type_id": node_type_id,
                        "num_workers": 2,
                        "data_security_mode": "USER_ISOLATION",
                        "spark_env_vars": {
                            "KEDRO_LOGGING_CONFIG": "\\${workspace.file_path}/conf/logging.yml"
                        },
                    },
                }
            ],
            "tasks": [{"task_key": default_key, "job_cluster_key": default_key}],
        }
    }
    conf_dir = metadata.project_path / "conf"
    databricks_config = _read_databricks_config(metadata.project_path)
    bundle_name = _get_bundle_name(databricks_config)
    targets = _get_targets(databricks_config)
    for target_name, target_conf in targets.items():
        target = DatabricksTarget(bundle_name, target_name, target_conf)
        target_conf_dir = conf_dir / target.name
        target_conf_dir.mkdir(exist_ok=True)
        target_config = kedro_databricks_config.copy()
        _create_target_config(target_config, target_conf_dir)
        _create_target_catalog(conf_dir, target_conf_dir, target.file_path)
        _create_gitkeep_file(target_conf_dir)
    log.info("Databricks targets created.")


class DatabricksTarget:
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
            warn=True,
        ).run()
        json_start = [
            i for i in range(len(result.stdout)) if result.stdout[i].startswith("{")
        ]
        if not json_start:  # pragma: no cover
            raise ValueError(f"Could not get metadata for target {self.name}")
        json_output = "\n".join(result.stdout[json_start[0] :])
        return json.loads(json_output)


def _substitute_file_path(string: str) -> str:
    """Substitute the file path in the catalog"""
    match = re.sub(
        r"(.*:)(.*)(data/.*)",
        r"\g<1> file://${_file_path}/\g<3>",
        string,
    )
    return match


def _read_databricks_config(project_path: Path) -> dict:
    with open(project_path / "databricks.yml") as f:
        conf = yaml.safe_load(f)
    return conf


def _get_bundle_name(config: dict) -> str:
    bundle_name = config.get("bundle", {}).get("name")
    if bundle_name is None:
        raise ValueError("No bundle name found in databricks.yml")
    return bundle_name


def _get_targets(config: dict) -> dict:
    targets = config.get("targets")
    if targets is None:
        raise ValueError("No targets found in databricks.yml")
    return targets


def _create_target_catalog(
    conf_dir: Path, target_conf_dir: Path, target_file_path: str
):  # pragma: no cover
    with open(f"{conf_dir}/base/catalog.yml") as f:
        cat = f.read()
    target_catalog = _substitute_file_path(cat)
    with open(target_conf_dir / "catalog.yml", "w") as f:
        f.write("_file_path: " + target_file_path + "\n" + target_catalog)


def _create_target_config(target_config, target_conf_dir):  # pragma: no cover
    with open(target_conf_dir / "databricks.yml", "w") as f:
        yaml.dump(target_config, f)


def _create_gitkeep_file(target_conf_dir):
    if not (target_conf_dir / ".gitkeep").exists():
        with open(target_conf_dir / ".gitkeep", "w") as f:
            f.write("")
