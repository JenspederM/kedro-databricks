from __future__ import annotations

import logging
import os
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


def go_to_project(metadata: ProjectMetadata) -> Path:
    """Change the current working directory to the project path.

    Args:
        metadata (ProjectMetadata): Project metadata.

    Returns:
        pathlib.Path: Path to the project directory.

    Raises:
        FileNotFoundError: If the project path does not exist.
    """
    project_path = Path(metadata.project_path)
    if not project_path.exists():
        raise FileNotFoundError(f"Project path {project_path} does not exist")
    os.chdir(project_path)
    return project_path


def validate_databricks_config(metadata: ProjectMetadata):
    """Check if the Databricks configuration file exists.

    Args:
        metadata (ProjectMetadata): Project metadata.

    Returns:
        bool: Whether the Databricks configuration file exists.

    Raises:
        FileNotFoundError: If the Databricks configuration file does not exist.
    """
    if not (metadata.project_path / "databricks.yml").exists():
        raise FileNotFoundError(_INVALID_CONFIG_MSG)
    return True


def create_dbfs_dir(metadata: ProjectMetadata, MSG: str):  # pragma: no cover
    """Create a directory in DBFS.

    Args:
        metadata (ProjectMetadata): Project metadata.
        MSG (str): Message to display.
    """
    run_cmd(
        ["databricks", "fs", "mkdirs", f"dbfs:/FileStore/{metadata.package_name}"],
        msg=MSG,
        warn=True,
    )


def upload_project_data(metadata: ProjectMetadata, MSG: str):  # pragma: no cover
    """Upload the project data to DBFS.

    Args:
        metadata (ProjectMetadata): Project metadata.
        MSG (str): Message to display.
    """
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
        [
            "databricks",
            "fs",
            "cp",
            "-r",
            "--overwrite",
            source_path.as_posix(),
            target_path,
        ],
        msg=MSG,
    )
    log.info(f"{MSG}: Data uploaded to {target_path}")


def upload_project_config(metadata: ProjectMetadata, conf: str, MSG: str):  # pragma: no cover
    """Upload the project configuration to DBFS.

    Args:
        metadata (ProjectMetadata): Project metadata.
        conf (str): The conf folder.
        MSG (str): Message to display.
    """
    package_name = metadata.package_name
    project_path = metadata.project_path
    log = logging.getLogger(package_name)

    with tarfile.open(project_path / f"dist/conf-{package_name}.tar.gz") as f:
        f.extractall("dist/", filter="tar")

    target_path = f"dbfs:/FileStore/{package_name}/{conf}"
    source_path = project_path / "dist" / conf
    if not source_path.exists():
        raise FileNotFoundError(f"Configuration path {source_path} does not exist")

    log.info(f"{MSG}: Uploading configuration to {target_path}")
    run_cmd(
        [
            "databricks",
            "fs",
            "cp",
            "-r",
            "--overwrite",
            source_path.as_posix(),
            target_path,
        ],
        msg=MSG,
    )
    log.info(f"{MSG}: Configuration uploaded to {target_path}")


def bundle_project(metadata: ProjectMetadata, env: str, MSG: str):  # pragma: no cover
    """Bundle the project.

    Args:
        metadata (ProjectMetadata): Project metadata.
        env (str): Environment to bundle.
        MSG (str): Message to display.
    """
    log = logging.getLogger(metadata.package_name)
    log.info(f"{MSG}: Running `kedro databricks bundle --env {env}`")
    bundle_cmd = ["kedro", "databricks", "bundle", "--env", env]
    run_cmd(bundle_cmd, msg=MSG)


def build_project(metadata: ProjectMetadata, MSG: str):  # pragma: no cover
    """Build the project.

    Args:
        metadata (ProjectMetadata): Project metadata.
        MSG (str): Message to display.
    """
    log = logging.getLogger(metadata.package_name)
    log.info(f"{MSG}: Building the project")
    go_to_project(metadata)
    build_cmd = ["kedro", "package"]
    result = run_cmd(build_cmd, msg=MSG)
    return result


def deploy_project(
    metadata: ProjectMetadata, MSG: str, target: str, debug: bool = False
):
    """Deploy the project to Databricks.

    Args:
        metadata (ProjectMetadata): Project metadata.
        MSG (str): Message to display.
        target (str): Databricks target environment to deploy to.
    """
    log = logging.getLogger(metadata.package_name)
    log.info(f"{MSG}: Running `databricks bundle deploy --target {target}`")
    deploy_cmd = ["databricks", "bundle", "deploy", "--target", target]
    if debug:
        deploy_cmd.append("--debug")
    run_cmd(deploy_cmd, msg=MSG)
    log.info(f"{MSG}: Deployment to Databricks succeeded")
