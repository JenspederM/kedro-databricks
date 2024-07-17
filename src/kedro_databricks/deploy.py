import logging
import os
import shutil
import tarfile
from pathlib import Path

from kedro.framework.startup import ProjectMetadata

from kedro_databricks.utils import run_cmd

_INVALID_CONFIG_MSG = """
No `databricks.yml` file found. Maybe you forgot to initialize the Databricks bundle?

You can initialize the Databricks bundle by running:

```
kedro databricks init
```
"""


def deploy_to_databricks(
    metadata: ProjectMetadata,
    env: str,
    bundle: bool = True,
    debug: bool = False,
):
    """Deploy the project to Databricks.

    Will bundle the project, upload the configuration and data to Databricks,
    and deploy the project to the specified environment.

    Args:
        metadata (ProjectMetadata): metadata of the project
        env (str): environment to deploy to
        bundle (bool): whether to bundle the project before deploying
        debug (bool): whether to run the deployment in debug mode
    """
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
    log.info("Deploying the project to Databricks...")
    deploy_cmd = ["databricks", "bundle", "deploy", "--target", env]
    if debug:
        deploy_cmd.append("--debug")
    run_cmd(deploy_cmd, msg="Failed to deploy the project")
    log.info("Project deployed successfully!")


def _go_to_project(path):
    project_path = Path(path)
    if not project_path.exists():
        raise FileNotFoundError(f"Project path {project_path} does not exist")
    os.chdir(project_path)
    return project_path


def _validate_databricks_config(project_path):
    if not (project_path / "databricks.yml").exists():
        raise FileNotFoundError(_INVALID_CONFIG_MSG)
    return True


def _upload_project_data(metadata: ProjectMetadata):  # pragma: no cover
    log = logging.getLogger(metadata.package_name)
    log.info("Uploading project data to Databricks...")
    data_path = metadata.project_path / "data"
    if not data_path.exists():
        log.warning(f"Data path {data_path} does not exist")
        return
    copy_data_cmd = [
        "databricks",
        "fs",
        "cp",
        "-r",
        str(data_path),
        f"dbfs:/FileStore/{metadata.package_name}/data",
    ]
    run_cmd(copy_data_cmd, msg="Failed to copy data to Databricks")


def _upload_project_config(metadata: ProjectMetadata):  # pragma: no cover
    log = logging.getLogger(metadata.package_name)
    log.info("Uploading project configuration to Databricks...")
    with tarfile.open(
        metadata.project_path / f"dist/conf-{metadata.package_name}.tar.gz"
    ) as f:
        f.extractall("dist/")

    run_cmd(
        ["databricks", "fs", "mkdirs", f"dbfs:/FileStore/{metadata.package_name}"],
        msg="Failed to create project directory on Databricks",
        warn=True,
    )

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
    run_cmd(copy_conf_cmd, msg="Failed to copy configuration to Databricks")


def _bundle_project(metadata: ProjectMetadata, env):
    log = logging.getLogger(metadata.package_name)
    log.info("Bundling the project...")
    bundle_cmd = ["kedro", "databricks", "bundle", "--env", env]
    run_cmd(bundle_cmd, msg="Failed to bundle the project")


def _build_project(metadata: ProjectMetadata):
    log = logging.getLogger(metadata.package_name)
    log.info("Building the project...")
    _go_to_project(metadata.project_path)
    build_cmd = ["kedro", "package"]
    result = run_cmd(build_cmd, msg="Failed to build the project")
    return result
