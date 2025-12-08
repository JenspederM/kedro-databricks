import json
import subprocess
import time
from collections.abc import Generator

import pytest
import yaml
from click.testing import CliRunner
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.cli.init.create_target_configs import (
    _get_targets,
    _read_databricks_config,
)
from kedro_databricks.plugin import commands
from tests.utils import reset_bundle, reset_init


@pytest.fixture
def kedro_project_with_init(
    cli_runner, metadata
) -> Generator[tuple[ProjectMetadata, CliRunner]]:
    reset_init(metadata)
    init_cmd = ["databricks", "init"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)
    assert metadata.project_path.exists(), "Project path not created"
    assert metadata.project_path.is_dir(), "Project path is not a directory"
    assert metadata.project_path / "databricks.yml", "Databricks config not created"

    databricks_config = _read_databricks_config(metadata.project_path)
    targets = _get_targets(databricks_config)
    for target in targets:
        override_path = metadata.project_path / "conf" / target / "databricks.yml"
        assert override_path.exists(), (
            f"Resource Overrides at {override_path} does not exist"
        )

    yield metadata, cli_runner
    reset_init(metadata)


@pytest.fixture
def kedro_project_with_init_bundle(
    kedro_project_with_init,
) -> Generator[tuple[ProjectMetadata, CliRunner]]:
    metadata, cli_runner = kedro_project_with_init
    reset_bundle(metadata)
    bundle_cmd = ["databricks", "bundle", "--env", "dev"]
    result = cli_runner.invoke(commands, bundle_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)
    yield metadata, cli_runner
    reset_bundle(metadata)


@pytest.fixture
def kedro_project_with_init_bundle_destroy(
    kedro_project_with_init_bundle,
) -> Generator[tuple[ProjectMetadata, CliRunner]]:
    """Initialize and destroy a Kedro Databricks project."""
    metadata, cli_runner = kedro_project_with_init_bundle
    yield metadata, cli_runner
    destroy_cmd = ["databricks", "destroy", "--", "--auto-approve"]
    result = cli_runner.invoke(commands, destroy_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)
    wait_for_job_deletion([])
    wait_for_volume_deletion(metadata)


@pytest.fixture
def kedro_project_with_init_destroy(
    kedro_project_with_init,
) -> Generator[tuple[ProjectMetadata, CliRunner]]:
    """Initialize and destroy a Kedro Databricks project."""
    metadata, cli_runner = kedro_project_with_init
    yield metadata, cli_runner
    destroy_cmd = ["databricks", "destroy", "--", "--auto-approve"]
    result = cli_runner.invoke(commands, destroy_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)
    wait_for_job_deletion([])
    wait_for_volume_deletion(metadata)


@pytest.fixture
def kedro_project_with_init_destroy_prod(
    kedro_project_with_init,
) -> Generator[tuple[ProjectMetadata, CliRunner]]:
    """Initialize and destroy a Kedro Databricks project."""
    metadata, cli_runner = kedro_project_with_init
    yield metadata, cli_runner
    destroy_cmd = ["databricks", "destroy", "--env", "prod", "--", "--auto-approve"]
    result = cli_runner.invoke(commands, destroy_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)
    wait_for_job_deletion([])
    wait_for_volume_deletion(metadata)


def wait_for_job_deletion(args) -> None:
    def _gather_jobs():
        jobs = subprocess.run(  # noqa: F821
            ["databricks", "jobs", "list", "--output", "json"] + args,
            check=True,
            capture_output=True,
            text=True,
        )
        if jobs.stdout:
            job_list = json.loads(jobs.stdout)
        else:
            job_list = []
        return [
            {
                "job_id": job["job_id"],
                "name": job["settings"]["name"],
            }
            for job in job_list
            if "develop_eggs" in job["settings"]["name"]
        ]

    def _delete_jobs(job_list):
        for j in job_list:
            res = subprocess.run(
                ["databricks", "jobs", "delete", str(j["job_id"])] + args,
                check=False,
                capture_output=True,
                text=True,
            )
            if res.returncode != 0:
                raise RuntimeError(f"Failed to delete job {j['name']}")

    job_list = _gather_jobs()
    max_tries = 12
    while job_list:
        max_tries -= 1
        if max_tries <= 0:
            raise TimeoutError("Timed out waiting for job deletion.")
        job_list = _gather_jobs()
        if job_list:
            _delete_jobs(job_list)
            time.sleep(5)
    print("All jobs have been deleted.")


def wait_for_volume_deletion(metadata: ProjectMetadata) -> None:
    resources_path = metadata.project_path / "resources"
    package_name = metadata.package_name
    volume_resources = list(resources_path.rglob("volumes.*.yml"))
    if not volume_resources:
        print("No volume resources found. Skipping volume deletion.")
        return
    elif len(volume_resources) > 1:
        raise ValueError("Multiple volume resource files found.")
    volume_path = resources_path / volume_resources[0].name
    with open(volume_path) as f:
        volume_resource = yaml.safe_load(f)
    volume_catalog = (
        volume_resource.get("resources", {})
        .get("volumes", {})
        .get(f"{package_name}_volume", {})
        .get("catalog_name")
    )
    volume_schema = (
        volume_resource.get("resources", {})
        .get("volumes", {})
        .get(f"{package_name}_volume", {})
        .get("schema_name")
    )
    volume_name = (
        volume_resource.get("resources", {})
        .get("volumes", {})
        .get(f"{package_name}_volume", {})
        .get("name")
    )
    if not volume_catalog or not volume_schema or not volume_name:
        raise ValueError("Volume resource information is incomplete.")
    volume_full_name = f"{volume_catalog}.{volume_schema}.{volume_name}"
    volume_exists = True
    max_tries = 12
    while volume_exists:
        max_tries -= 1
        if max_tries <= 0:
            raise TimeoutError("Timed out waiting for volume deletion.")
        volume_exists = (
            subprocess.run(
                ["databricks", "volumes", "read", volume_full_name, "--output", "json"],
                check=False,
                capture_output=True,
                text=True,
            ).stdout.strip()
            and True
            or False
        )
        if volume_exists:
            subprocess.run(
                ["databricks", "volumes", "delete", volume_full_name],
                check=True,
            )
            time.sleep(5)
    print(f"Volume {volume_full_name} has been deleted.")
