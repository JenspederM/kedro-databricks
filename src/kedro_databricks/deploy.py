import logging
import os
import shutil
import subprocess
import tarfile
from pathlib import Path

from kedro.framework.startup import ProjectMetadata


def deploy_to_databricks(
    metadata: ProjectMetadata,
    env: str,
    bundle: bool = True,
    debug: bool = False,
):
    log = logging.getLogger(metadata.package_name)
    if shutil.which("databricks") is None:  # pragma: no cover
        raise Exception("databricks CLI is not installed")

    project_path = _go_to_project(metadata.project_path)
    _validate_databricks_config(project_path)
    _build_project(metadata)
    if bundle is True:
        _bundle_project(metadata, env)
    _upload_project_config(metadata)
    _upload_project_data(metadata)
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


def _upload_project_data(metadata: ProjectMetadata):  # pragma: no cover
    log = logging.getLogger(metadata.package_name)
    log.info("Upload project data to Databricks...")
    data_path = metadata.project_path / "data"
    if data_path.exists():
        copy_data_cmd = [
            "databricks",
            "fs",
            "cp",
            "-r",
            str(data_path),
            f"dbfs:/FileStore/{metadata.package_name}/data",
        ]
        result = subprocess.run(copy_data_cmd, check=False, capture_output=True)
        if result.returncode != 0:
            raise Exception(f"Failed to copy data to Databricks: {result.stderr}")


def _upload_project_config(metadata: ProjectMetadata):  # pragma: no cover
    log = logging.getLogger(metadata.package_name)
    log.info("Upload project configuration to Databricks...")
    with tarfile.open(
        metadata.project_path / f"dist/conf-{metadata.package_name}.tar.gz"
    ) as f:
        f.extractall("dist/")

    try:
        remove_cmd = [
            "databricks",
            "fs",
            "rm",
            "-r",
            f"dbfs:/FileStore/{metadata.package_name}",
        ]
        result = subprocess.run(remove_cmd, check=False)
        if result.returncode != 0:
            log.warning(f"Failed to remove existing project: {result.stderr}")
    except Exception as e:
        log.warning(f"Failed to remove existing project: {e}")

    conf_path = metadata.project_path / "dist" / "conf"
    if not conf_path.exists():
        raise FileNotFoundError(f"Configuration path {conf_path} does not exist")

    copy_conf_cmd = [
        "databricks",
        "fs",
        "cp",
        "-r",
        str(conf_path),
        f"dbfs:/FileStore/{metadata.package_name}/conf",
    ]
    result = subprocess.run(copy_conf_cmd, check=False, capture_output=True)
    if result.returncode != 0:
        raise Exception(f"Failed to copy configuration to Databricks: {result.stderr}")


def _bundle_project(metadata: ProjectMetadata, env):
    log = logging.getLogger(metadata.package_name)
    log.info("Bundling the project...")
    bundle_cmd = ["kedro", "databricks", "bundle", "--env", env]
    result = subprocess.run(bundle_cmd, capture_output=True, check=True)
    if result.returncode != 0:
        raise Exception(f"Failed to bundle the project: {result.stderr}")


def _build_project(metadata: ProjectMetadata):
    log = logging.getLogger(metadata.package_name)
    log.info("Building the project...")
    _go_to_project(metadata.project_path)
    build_cmd = ["kedro", "package"]
    result = subprocess.run(build_cmd, capture_output=True, check=True)
    if result.returncode != 0:
        raise Exception(f"Failed to build the project: {result.stderr}")
    return result
