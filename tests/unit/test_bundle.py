from __future__ import annotations

import shutil

import yaml

from kedro_databricks.commands.bundle import save_resources
from kedro_databricks.constants import (
    DEFAULT_CONF_FOLDER,
    DEFAULT_CONFIG_GENERATOR,
    DEFAULT_CONFIG_KEY,
)
from kedro_databricks.plugin import commands
from kedro_databricks.utilities.common import get_arg_value
from kedro_databricks.utilities.resource_generator import (
    NodeResourceGenerator,
)
from tests.utils import reset_project, validate_bundle


def task_validator(tasks):
    assert len(tasks) == 8
    for i, task in enumerate(tasks):
        assert task.get("task_key") in (f"node{i}", f"ns_{i}_node_{i}_1")
        params = task.get("python_wheel_task").get("parameters")
        assert params is not None
        env = get_arg_value(params, "--env")
        assert env is not None
        assert env == "${var.environment}"


def test_bundle(metadata, cli_runner):
    # Arrange
    reset_project(metadata)
    empty_overrides = {"resources": {"jobs": {}}}
    (metadata.project_path / "conf" / "fake_env").mkdir(parents=True, exist_ok=True)
    with open(
        metadata.project_path / "conf" / "fake_env" / "databricks.yml",
        "w",
    ) as f:
        yaml.dump(empty_overrides, f)

    # Act
    result = cli_runner.invoke(
        commands,
        [
            "databricks",
            "bundle",
            "--env",
            "fake_env",
            "--default-key",
            DEFAULT_CONFIG_KEY,
            "--resource-generator",
            DEFAULT_CONFIG_GENERATOR,
            "--conf-source",
            DEFAULT_CONF_FOLDER,
            "--overwrite",
        ],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)
    validate_bundle(
        metadata=metadata,
        required_files=[
            f"jobs.{metadata.package_name}.yml",
            f"jobs.{metadata.package_name}_namespaced_pipeline.yml",
            f"jobs.{metadata.package_name}_ds.yml",
        ],
        task_validator=task_validator,
    )

    # Cleanup
    shutil.rmtree(metadata.project_path / "conf" / "fake_env")


def test_bundle_no_overrides(metadata, cli_runner):
    # Act
    result = cli_runner.invoke(
        commands,
        [
            "databricks",
            "bundle",
            "--env",
            "fake_env",
            "--default-key",
            DEFAULT_CONFIG_KEY,
            "--resource-generator",
            DEFAULT_CONFIG_GENERATOR,
            "--overwrite",
        ],
    )

    # Assert
    assert result.exit_code == 1, (result.exit_code, result.stdout, result.exception)


def test_bundle_invalid_resource_generator(metadata, cli_runner):
    # Act
    result = cli_runner.invoke(
        commands,
        [
            "databricks",
            "bundle",
            "--env",
            "fake_env",
            "--default-key",
            DEFAULT_CONFIG_KEY,
            "--resource-generator",
            "invalid",
            "--overwrite",
        ],
    )

    # Assert
    assert result.exit_code == 1, (result.exit_code, result.stdout, result.exception)


def test_save_resources(metadata):
    # Arrange
    controller = NodeResourceGenerator(metadata, "fake_env")
    jobs = controller.generate_jobs()
    resources = {"resources": {"jobs": jobs}}

    # Act
    save_resources(metadata, resources, True)

    # Assert
    resource_dir = metadata.project_path / "resources"
    assert resource_dir.exists(), "Failed to create resources directory"
    assert resource_dir.is_dir(), "resouces is not a directory"
    for job in jobs:
        job_file = resource_dir / f"jobs.{job}.yml"
        assert job_file.exists(), f"Failed to save job resource: {job_file}"
