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
    MSG = "Deploying to Databricks"
    package_name = metadata.package_name
    project_path = metadata.project_path
    log = logging.getLogger(package_name)
    if shutil.which("databricks") is None:  # pragma: no cover
        raise Exception("databricks CLI is not installed")

    project_path = _go_to_project(metadata.project_path)
    _validate_databricks_config(project_path)
    _build_project(metadata, MSG=MSG)
    if bundle is True:
        _bundle_project(metadata, env, MSG=MSG)
    _create_dbfs_dir(metadata, MSG=MSG)
    _upload_project_config(metadata, MSG=MSG)
    _upload_project_data(metadata, MSG=MSG)
    log.info(f"{MSG}: Running `databricks bundle deploy --target {env}`")
    deploy_cmd = ["databricks", "bundle", "deploy", "--target", env]
    if debug:
        deploy_cmd.append("--debug")
    run_cmd(deploy_cmd, msg=MSG)
    log.info(f"{MSG}: Deployment to Databricks succeeded")


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


def _create_dbfs_dir(metadata: ProjectMetadata, MSG: str):  # pragma: no cover
    run_cmd(
        ["databricks", "fs", "mkdirs", f"dbfs:/FileStore/{metadata.package_name}"],
        msg=MSG,
        warn=True,
    )


def _upload_project_data(metadata: ProjectMetadata, MSG: str):  # pragma: no cover
    package_name = metadata.package_name
    project_path = metadata.project_path
    log = logging.getLogger(package_name)
    target_path = f"dbfs:/FileStore/{package_name}/data"
    source_path = project_path / "data"
    if not source_path.exists():
        log.warning(f"Data path {source_path} does not exist")
        return

    log.info(
        f"{MSG}: Uploading {source_path.relative_to(project_path)} to {target_path}"
    )
    run_cmd(
        ["databricks", "fs", "cp", "-r", source_path.as_posix(), target_path], msg=MSG
    )
    log.info(f"{MSG}: Data uploaded to {target_path}")


def _upload_project_config(metadata: ProjectMetadata, MSG: str):  # pragma: no cover
    package_name = metadata.package_name
    project_path = metadata.project_path
    log = logging.getLogger(package_name)

    with tarfile.open(project_path / f"dist/conf-{package_name}.tar.gz") as f:
        f.extractall("dist/")

    target_path = f"dbfs:/FileStore/{package_name}/conf"
    source_path = project_path / "dist" / "conf"
    if not source_path.exists():
        raise FileNotFoundError(f"Configuration path {source_path} does not exist")

    log.info(f"{MSG}: Uploading configuration to Databricks")
    run_cmd(
        ["databricks", "fs", "cp", "-r", source_path.as_posix(), target_path], msg=MSG
    )
    log.info(f"{MSG}: Configuration uploaded to {target_path}")


def _bundle_project(metadata: ProjectMetadata, env: str, MSG: str):  # pragma: no cover
    log = logging.getLogger(metadata.package_name)
    log.info(f"{MSG}: Running `kedro databricks bundle --env {env}`")
    bundle_cmd = ["kedro", "databricks", "bundle", "--env", env]
    run_cmd(bundle_cmd, msg=MSG)


def _build_project(metadata: ProjectMetadata, MSG: str):  # pragma: no cover
    log = logging.getLogger(metadata.package_name)
    log.info(f"{MSG}: Building the project")
    _go_to_project(metadata.project_path)
    build_cmd = ["kedro", "package"]
    result = run_cmd(build_cmd, msg=MSG)
    return result
