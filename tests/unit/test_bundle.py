from __future__ import annotations

import shutil

import yaml

from kedro_databricks.constants import (
    DEFAULT_CONF_FOLDER,
    DEFAULT_CONFIG_GENERATOR,
    DEFAULT_CONFIG_KEY,
    DEFAULT_ENV,
)
from kedro_databricks.plugin import commands
from kedro_databricks.utilities.common import get_arg_value
from tests.utils import reset_project, validate_bundle, write_catalog


def task_validator(tasks):
    assert len(tasks) == 8
    for i, task in enumerate(tasks):
        assert task.get("task_key") in (f"node{i}", f"ns_{i}_node_{i}_1")
        params = task.get("python_wheel_task").get("parameters")
        assert params is not None
        env = get_arg_value(params, "--env")
        assert env is not None
        assert env == "${var.environment}"


def test_bundle(cli_runner, metadata):
    # Arrange
    reset_project(metadata)
    write_catalog(metadata, DEFAULT_ENV)
    empty_overrides = {"resources": {"jobs": {}}}
    (metadata.project_path / "conf" / DEFAULT_ENV).mkdir(parents=True, exist_ok=True)
    with open(
        metadata.project_path / "conf" / DEFAULT_ENV / "databricks.yml",
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
            DEFAULT_ENV,
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
        env=DEFAULT_ENV,
        required_files=[
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}.yml",
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}_namespaced_pipeline.yml",
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}_ds.yml",
        ],
        task_validator=task_validator,
    )

    # Cleanup
    shutil.rmtree(metadata.project_path / "conf" / DEFAULT_ENV)


def test_bundle_default_with_memory_datasets(cli_runner, metadata):
    # Arrange
    reset_project(metadata)
    empty_overrides = {"resources": {"jobs": {}}}
    (metadata.project_path / "conf" / DEFAULT_ENV).mkdir(parents=True, exist_ok=True)
    with open(
        metadata.project_path / "conf" / DEFAULT_ENV / "databricks.yml",
        "w",
    ) as f:
        yaml.dump(empty_overrides, f)

    with open(metadata.project_path / "conf" / "base" / "catalog.yml", "w") as f:
        f.write("")
    with open(metadata.project_path / "conf" / DEFAULT_ENV / "catalog.yml", "w") as f:
        f.write("")

    result = cli_runner.invoke(
        commands,
        [
            "databricks",
            "bundle",
            "--env",
            DEFAULT_ENV,
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

    assert result.exit_code == 1, "bundle should fail"
    assert (
        "Resource Generator of type NodeResourceGenerator does not support MemoryDatasets"
        in str(result.exception)
    ), f"bundle should fail with MemoryDatasetError, not {result.exception}"


def test_bundle_default_with_pipeline_generator(cli_runner, metadata):
    # Arrange
    reset_project(metadata)
    empty_overrides = {"resources": {"jobs": {}}}
    (metadata.project_path / "conf" / DEFAULT_ENV).mkdir(parents=True, exist_ok=True)
    with open(
        metadata.project_path / "conf" / DEFAULT_ENV / "databricks.yml",
        "w",
    ) as f:
        yaml.dump(empty_overrides, f)

    with open(metadata.project_path / "conf" / "base" / "catalog.yml", "w") as f:
        f.write("")
    with open(metadata.project_path / "conf" / DEFAULT_ENV / "catalog.yml", "w") as f:
        f.write("")

    result = cli_runner.invoke(
        commands,
        [
            "databricks",
            "bundle",
            "--env",
            DEFAULT_ENV,
            "--default-key",
            DEFAULT_CONFIG_KEY,
            "--resource-generator",
            "pipeline",
            "--conf-source",
            DEFAULT_CONF_FOLDER,
            "--overwrite",
        ],
        obj=metadata,
    )

    assert result.exit_code == 0, "bundle should not fail with pipeline generator"
    validate_bundle(
        metadata=metadata,
        env=DEFAULT_ENV,
        required_files=[
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}.yml",
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}_namespaced_pipeline.yml",
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}_ds.yml",
        ],
        task_validator=task_validator,
    )


def test_bundle_no_overrides(cli_runner, metadata):
    # Act
    result = cli_runner.invoke(
        commands,
        [
            "databricks",
            "bundle",
            "--env",
            DEFAULT_ENV,
            "--default-key",
            DEFAULT_CONFIG_KEY,
            "--resource-generator",
            DEFAULT_CONFIG_GENERATOR,
            "--overwrite",
        ],
    )

    # Assert
    assert result.exit_code == 1, (result.exit_code, result.stdout, result.exception)


def test_bundle_invalid_resource_generator(cli_runner, metadata):
    # Act
    result = cli_runner.invoke(
        commands,
        [
            "databricks",
            "bundle",
            "--env",
            DEFAULT_ENV,
            "--default-key",
            DEFAULT_CONFIG_KEY,
            "--resource-generator",
            "invalid",
            "--overwrite",
        ],
    )

    # Assert
    assert result.exit_code == 1, (result.exit_code, result.stdout, result.exception)
