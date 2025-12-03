import subprocess
import time
from collections.abc import Generator

import pytest
import yaml
from click.testing import CliRunner
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.plugin import commands
from kedro_databricks.utils import get_targets, read_databricks_config
from tests.utils import reset_bundle, reset_init


@pytest.fixture
def kedro_project_with_init(
    cli_runner, metadata, custom_provider
) -> Generator[tuple[ProjectMetadata, CliRunner]]:
    reset_init(metadata)
    init_cmd = ["databricks", "init", "--provider", custom_provider]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)
    assert metadata.project_path.exists(), "Project path not created"
    assert metadata.project_path.is_dir(), "Project path is not a directory"
    assert metadata.project_path / "databricks.yml", "Databricks config not created"

    databricks_config = read_databricks_config(metadata.project_path)
    targets = get_targets(databricks_config)
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
    destroy_cmd = ["databricks", "destroy", "--auto-approve"]
    result = cli_runner.invoke(commands, destroy_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)
    wait_for_volume_deletion(metadata)


@pytest.fixture
def kedro_project_with_init_destroy(
    kedro_project_with_init,
) -> Generator[tuple[ProjectMetadata, CliRunner]]:
    """Initialize and destroy a Kedro Databricks project."""
    metadata, cli_runner = kedro_project_with_init
    yield metadata, cli_runner
    destroy_cmd = ["databricks", "destroy", "--auto-approve"]
    result = cli_runner.invoke(commands, destroy_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)
    wait_for_volume_deletion(metadata)


def wait_for_volume_deletion(metadata: ProjectMetadata) -> None:
    resources_path = metadata.project_path / "resources"
    package_name = metadata.package_name
    volume_path = resources_path / next(resources_path.rglob("volumes.*.yml")).name
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
        print("Volume resource information is incomplete.")
        return
    volume_full_name = f"{volume_catalog}.{volume_schema}.{volume_name}"
    volume_exists = True
    max_tries = 12
    while volume_exists:
        max_tries -= 1
        if max_tries <= 0:
            print(f"Volume {volume_full_name} still exists after maximum retries.")
            break
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
