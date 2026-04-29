from __future__ import annotations

import shutil

import yaml

from kedro_databricks.config import config
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
    write_catalog(metadata, config.default_env)
    empty_overrides = {"resources": {"jobs": {}}}
    (metadata.project_path / "conf" / config.default_env).mkdir(
        parents=True, exist_ok=True
    )
    with open(
        metadata.project_path / "conf" / config.default_env / "databricks.yml",
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
            config.default_env,
            "--default-key",
            config.workflow_default_key,
            "--resource-generator",
            config.workflow_generator,
            "--conf-source",
            config.conf_source,
            "--overwrite",
        ],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)
    validate_bundle(
        metadata=metadata,
        env=config.default_env,
        required_files=[
            f"target.{config.default_env}.jobs.{metadata.package_name}.yml",
            f"target.{config.default_env}.jobs.{metadata.package_name}_namespaced_pipeline.yml",
            f"target.{config.default_env}.jobs.{metadata.package_name}_ds.yml",
        ],
        task_validator=task_validator,
    )

    # Cleanup
    shutil.rmtree(metadata.project_path / "conf" / config.default_env)


def test_bundle_default_with_memory_datasets(cli_runner, metadata):
    # Arrange
    reset_project(metadata)
    empty_overrides = {"resources": {"jobs": {}}}
    (metadata.project_path / "conf" / config.default_env).mkdir(
        parents=True, exist_ok=True
    )
    with open(
        metadata.project_path / "conf" / config.default_env / "databricks.yml",
        "w",
    ) as f:
        yaml.dump(empty_overrides, f)

    with open(metadata.project_path / "conf" / "base" / "catalog.yml", "w") as f:
        f.write("")
    with open(
        metadata.project_path / "conf" / config.default_env / "catalog.yml", "w"
    ) as f:
        f.write("")

    result = cli_runner.invoke(
        commands,
        [
            "databricks",
            "bundle",
            "--env",
            config.default_env,
            "--default-key",
            config.workflow_default_key,
            "--resource-generator",
            config.workflow_generator,
            "--conf-source",
            config.conf_source,
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
    (metadata.project_path / "conf" / config.default_env).mkdir(
        parents=True, exist_ok=True
    )
    with open(
        metadata.project_path / "conf" / config.default_env / "databricks.yml",
        "w",
    ) as f:
        yaml.dump(empty_overrides, f)

    with open(metadata.project_path / "conf" / "base" / "catalog.yml", "w") as f:
        f.write("")
    with open(
        metadata.project_path / "conf" / config.default_env / "catalog.yml", "w"
    ) as f:
        f.write("")

    result = cli_runner.invoke(
        commands,
        [
            "databricks",
            "bundle",
            "--env",
            config.default_env,
            "--default-key",
            config.workflow_default_key,
            "--resource-generator",
            "pipeline",
            "--conf-source",
            config.conf_source,
            "--overwrite",
        ],
        obj=metadata,
    )

    assert result.exit_code == 0, "bundle should not fail with pipeline generator"
    validate_bundle(
        metadata=metadata,
        env=config.default_env,
        required_files=[
            f"target.{config.default_env}.jobs.{metadata.package_name}.yml",
            f"target.{config.default_env}.jobs.{metadata.package_name}_namespaced_pipeline.yml",
            f"target.{config.default_env}.jobs.{metadata.package_name}_ds.yml",
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
            config.default_env,
            "--default-key",
            config.workflow_default_key,
            "--resource-generator",
            config.workflow_generator,
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
            config.default_env,
            "--default-key",
            config.workflow_default_key,
            "--resource-generator",
            "invalid",
            "--overwrite",
        ],
    )

    # Assert
    assert result.exit_code == 1, (result.exit_code, result.stdout, result.exception)
