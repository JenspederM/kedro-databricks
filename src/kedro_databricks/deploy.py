import logging
import os
import shutil
import subprocess
import tarfile
from pathlib import Path

from kedro.framework.startup import ProjectMetadata

from kedro_databricks import LOGGING_NAME

log = logging.getLogger(LOGGING_NAME)


def deploy_to_databricks(
    metadata: ProjectMetadata,
    env: str,
    bundle: bool = True,
):
    if shutil.which("databricks") is None:  # pragma: no cover
        raise Exception("databricks CLI is not installed")

    project_path = _go_to_project(metadata.project_path)
    _validate_databricks_config(project_path)
    _build_project(metadata)
    if bundle is True:
        _bundle_project(env)
    _upload_project_config(metadata.package_name, project_path)
    _upload_project_data(metadata.package_name, project_path)
    deploy_cmd = ["databricks", "bundle", "deploy", "--target", env]
    result = subprocess.run(deploy_cmd, check=True, capture_output=True)
    if result.returncode != 0:
        raise Exception(f"Failed to deploy the project: {result.stderr}")
    log.info("Project deployed successfully!")


def _go_to_project(path):
    project_path = Path(path)
    if not project_path.exists():
        raise FileNotFoundError(f"Project path {project_path} does not exist")
    os.chdir(project_path)
    return project_path


def _validate_databricks_config(project_path):
    if not (project_path / "databricks.yml").exists():
        raise FileNotFoundError(
            f"Configuration file {project_path / 'databricks.yml'} does not exist"
        )
    return True


def _upload_project_data(package_name, project_path):  # pragma: no cover
    log.info("Upload project data to Databricks...")
    data_path = project_path / "data"
    if data_path.exists():
        copy_data_cmd = [
            "databricks",
            "fs",
            "cp",
            "-r",
            str(data_path),
            f"dbfs:/FileStore/{package_name}/data",
        ]
        result = subprocess.run(copy_data_cmd, check=False, capture_output=True)
        if result.returncode != 0:
            raise Exception(f"Failed to copy data to Databricks: {result.stderr}")


def _upload_project_config(package_name, project_path):  # pragma: no cover
    log.info("Upload project configuration to Databricks...")
    with tarfile.open(project_path / f"dist/conf-{package_name}.tar.gz") as f:
        f.extractall("dist/")

    try:
        remove_cmd = ["databricks", "fs", "rm", "-r", f"dbfs:/FileStore/{package_name}"]
        result = subprocess.run(remove_cmd, check=False)
        if result.returncode != 0:
            log.warning(f"Failed to remove existing project: {result.stderr}")
    except Exception as e:
        log.warning(f"Failed to remove existing project: {e}")

    conf_path = project_path / "dist" / "conf"
    if not conf_path.exists():
        raise FileNotFoundError(f"Configuration path {conf_path} does not exist")

    copy_conf_cmd = [
        "databricks",
        "fs",
        "cp",
        "-r",
        str(conf_path),
        f"dbfs:/FileStore/{package_name}/conf",
    ]
    result = subprocess.run(copy_conf_cmd, check=False, capture_output=True)
    if result.returncode != 0:
        raise Exception(f"Failed to copy configuration to Databricks: {result.stderr}")


def _bundle_project(env):
    log.info("Bundling the project...")
    bundle_cmd = ["kedro", "databricks", "bundle", "--env", env]
    result = subprocess.run(bundle_cmd, capture_output=True, check=True)
    if result.returncode != 0:
        raise Exception(f"Failed to bundle the project: {result.stderr}")


def _build_project(metadata: ProjectMetadata):
    log.info("Building the project...")
    _go_to_project(metadata.project_path)
    build_cmd = ["kedro", "package"]
    result = subprocess.run(build_cmd, capture_output=True, check=True)
    if result.returncode != 0:
        raise Exception(f"Failed to build the project: {result.stderr}")
    return result
