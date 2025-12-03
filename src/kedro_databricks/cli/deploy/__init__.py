from __future__ import annotations

import os
from pathlib import Path
from subprocess import CompletedProcess

from kedro.framework.startup import ProjectMetadata

from kedro_databricks.cli.deploy.get_deployed_resources import get_deployed_resources
from kedro_databricks.logger import get_logger
from kedro_databricks.utils import (
    Command,
    _get_arg_value,
    assert_databricks_cli,
    get_env_file_path,
)

log = get_logger("deploy")


def deploy(metadata: ProjectMetadata, env: str, *databricks_args: str):
    """Deploy the Databricks Asset Bundle.

    This function deploys the Databricks Asset Bundle in the current project
    directory. It also creates a Databricks configuration file and a
    Databricks target configuration file.

    Args:
        metadata (ProjectMetadata): The project metadata.
        env (str): The environment to deploy to.
        *databricks_args: Additional arguments to be passed to the `databricks` CLI.

    Raises:
        RuntimeError: If the `databricks` CLI is not installed or the wrong version is used.
    """
    _validate_project(metadata)
    os.chdir(metadata.project_path)
    _deploy_project(metadata, env, list(databricks_args))
    result = _upload_project_data(metadata, env, list(databricks_args))
    if result and result.returncode != 0:  # pragma: no cover
        raise RuntimeError("Failed to upload project data to DBFS")


def _validate_project(metadata: ProjectMetadata):
    assert_databricks_cli()
    if not metadata.project_path.exists():
        raise FileNotFoundError(f"Project path {metadata.project_path} does not exist")
    if not (Path(metadata.project_path) / "databricks.yml").exists():
        raise FileNotFoundError(
            "Databricks configuration file does not exist. "
            "Please run `kedro databricks init` to create it."
        )


# TODO: Add tests
def _upload_project_data(
    metadata: ProjectMetadata, env: str, databricks_args: list[str]
):  # pragma: no cover
    """Upload the project data to DBFS."""

    source_path = metadata.project_path / "data"
    if not source_path.exists():
        log.warning(f"Data path {source_path} does not exist")
        return
    file_path = get_env_file_path(metadata, env)
    if not file_path:
        log.warning(f"No file path found for the given environment: '{env}'")
        return
    target_path = f"dbfs:{file_path}/data"
    log.info(
        f"Uploading {source_path.relative_to(metadata.project_path)} to {target_path}"
    )
    upload_cmd = [
        "databricks",
        "fs",
        "cp",
        "-r",
        "--overwrite",
        source_path.as_posix(),
        target_path,
    ] + databricks_args
    target = _get_arg_value(databricks_args, "--target")
    if target is None:
        upload_cmd += ["--target", env]
    result = Command(upload_cmd, log=log).run(cwd=metadata.project_path)
    log.info(f"Data uploaded to {target_path}")
    return result


# TODO: Add tests
def _deploy_project(
    metadata: ProjectMetadata, env: str, databricks_args: list[str]
):  # pragma: no cover
    """Deploy the project to Databricks.

    Args:
        databricks_args (list[str]): Databricks arguments.

    Returns:
        subprocess.CompletedProcess: The result of the deployment.
    """
    deploy_cmd = ["databricks", "bundle", "deploy"] + databricks_args
    target = _get_arg_value(databricks_args, "--target")
    if target is None:
        deploy_cmd += ["--target", env]
    result = Command(deploy_cmd, log=log, warn=True).run(cwd=metadata.project_path)
    # databricks bundle deploy logs to stderr for some reason.
    if _check_deployment_complete(result):
        result.returncode = 0
    else:
        raise RuntimeError("Deployment failed. Check the logs for more details.")
    log.info("Successfully Deployed Jobs")
    get_deployed_resources(metadata, only_dev=target in ["dev", "local"])
    return result


def _check_deployment_complete(result: CompletedProcess) -> bool:
    complete_stdout = result.stdout and "Deployment complete!\n" in result.stdout
    complete_stderr = result.stderr and "Deployment complete!\n" in result.stderr
    return complete_stdout is True or complete_stderr is True
