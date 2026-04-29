import shutil

import yaml

from kedro_databricks.commands.bundle import (
    NoOverridesError,
    NoResourcesKeyError,
    command,
)
from kedro_databricks.config import config
from kedro_databricks.utilities.common import get_arg_value
from tests.utils import init_project, reset_project, validate_bundle


def test_databricks_bundle_with_overrides(metadata, cli_runner):
    """Test the `bundle` command"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)

    # Act
    result = cli_runner.invoke(
        command,
        [
            "--env",
            config.default_env,
        ],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)

    def task_validator(tasks):
        assert len(tasks) == 8
        for i, task in enumerate(tasks):
            assert task.get("task_key") in (f"node{i}", f"ns_{i}_node_{i}_1")
            assert task.get("environment_key") == config.workflow_default_key
            params = task.get("python_wheel_task").get("parameters")
            assert params is not None
            env = get_arg_value(params, "--env")
            assert env is not None
            assert env == "${var.environment}"

    validate_bundle(
        metadata=metadata,
        env=config.default_env,
        required_files=[
            f"target.{config.default_env}.jobs.{metadata.package_name}.yml",
            f"target.{config.default_env}.jobs.{metadata.package_name}_ds.yml",
            f"target.{config.default_env}.jobs.{metadata.package_name}_namespaced_pipeline.yml",
            f"target.{config.default_env}.volumes.{metadata.package_name}_volume.yml",
        ],
        task_validator=task_validator,
    )


def test_databricks_bundle_with_conf(metadata, cli_runner):
    """Test the `bundle` command"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)
    CONF_KEY = "custom_conf"
    default_path = (
        metadata.project_path / "conf" / config.default_env / "databricks.yml"
    )
    override_path = (
        metadata.project_path / CONF_KEY / config.default_env / "databricks.yml"
    )
    override_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(default_path, override_path)
    settings = metadata.project_path / "src" / metadata.package_name / "settings.py"
    original_settings = settings.read_text()
    with open(settings, "a") as f:
        f.write(f"\nCONF_SOURCE = '{CONF_KEY}'")

    # Act
    result = cli_runner.invoke(
        command,
        [
            "--env",
            config.default_env,
            "--conf-source",
            CONF_KEY,
        ],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)

    def task_validator(tasks):
        assert len(tasks) == 8
        for i, task in enumerate(tasks):
            assert task.get("task_key") in (f"node{i}", f"ns_{i}_node_{i}_1")
            assert task.get("environment_key") == config.workflow_default_key
            params = task.get("python_wheel_task").get("parameters")
            assert params is not None
            env = get_arg_value(params, "--env")
            assert env is not None
            assert env == "${var.environment}"
            conf_source = get_arg_value(params, "--conf-source")
            assert conf_source is not None
            assert conf_source == f"/${{workspace.file_path}}/{CONF_KEY}"

    validate_bundle(
        metadata=metadata,
        env=config.default_env,
        required_files=[
            f"target.{config.default_env}.jobs.{metadata.package_name}.yml",
            f"target.{config.default_env}.jobs.{metadata.package_name}_ds.yml",
            f"target.{config.default_env}.jobs.{metadata.package_name}_namespaced_pipeline.yml",
            f"target.{config.default_env}.volumes.{metadata.package_name}_volume.yml",
        ],
        task_validator=task_validator,
    )

    # Cleanup
    settings.write_text(original_settings)
    shutil.rmtree(metadata.project_path / CONF_KEY)


def test_databricks_bundle_without_overrides(metadata, cli_runner):
    """Test the `bundle` command"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)
    shutil.rmtree(metadata.project_path / "conf" / config.default_env)

    # Act
    result = cli_runner.invoke(
        command,
        [
            "--env",
            config.default_env,
        ],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 1, (result.exit_code, result.stdout, result.exception)
    assert isinstance(result.exception, NoOverridesError)
    exception = bytes(str(result.exception), "utf-8").decode("unicode_escape")
    assert "Could not find any override definitions" in exception


def test_databricks_bundle_no_resources_key(metadata, cli_runner):
    """Test the `bundle` command"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)
    conf_path = metadata.project_path / "conf" / config.default_env / "databricks.yml"
    with open(conf_path, "w") as f:
        yaml.dump({"default": {"my-job": {"overrides": "test"}}}, f)

    # Act
    result = cli_runner.invoke(
        command,
        [
            "--env",
            config.default_env,
        ],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 1, (result.exit_code, result.stdout, result.exception)
    assert isinstance(result.exception, NoResourcesKeyError)
    exception = bytes(str(result.exception), "utf-8").decode("unicode_escape")
    assert "'resources' key not found in the 'databricks' configuration" in exception


def test_databricks_bundle_with_params(metadata, cli_runner):
    """Test the `bundle` command"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)

    # Act
    result = cli_runner.invoke(
        command,
        [
            "--env",
            config.default_env,
            "--params",
            "run_date={{job.parameters.run_date}},run_id={{job.parameters.run_id}}",
            "--overwrite",
        ],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)

    def task_validator(tasks):
        assert len(tasks) == 8
        for i, task in enumerate(tasks):
            assert task.get("task_key") in (f"node{i}", f"ns_{i}_node_{i}_1")
            assert task.get("environment_key") == config.workflow_default_key
            params = task.get("python_wheel_task").get("parameters")
            assert params is not None
            value = get_arg_value(params, "--params")
            assert value is not None
            assert (
                value
                == "run_date={{job.parameters.run_date}},run_id={{job.parameters.run_id}}"
            )

    validate_bundle(
        metadata=metadata,
        env=config.default_env,
        required_files=[
            f"target.{config.default_env}.jobs.{metadata.package_name}.yml",
            f"target.{config.default_env}.jobs.{metadata.package_name}_ds.yml",
            f"target.{config.default_env}.jobs.{metadata.package_name}_namespaced_pipeline.yml",
            f"target.{config.default_env}.volumes.{metadata.package_name}_volume.yml",
        ],
        task_validator=task_validator,
    )


def test_databricks_bundle_with_pipeline(metadata, cli_runner):
    """Test the `bundle` command"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)

    # Act
    result = cli_runner.invoke(
        command,
        [
            "--env",
            config.default_env,
            "--pipeline",
            "ds",
            "--overwrite",
        ],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)

    def task_validator(tasks):
        assert len(tasks) == 8
        for i, task in enumerate(tasks):
            assert task.get("task_key") in (f"node{i}", f"ns_{i}_node_{i}_1")
            assert task.get("environment_key") == config.workflow_default_key
            params = task.get("python_wheel_task").get("parameters")
            assert params is not None
            env = get_arg_value(params, "--env")
            assert env is not None
            assert env == "${var.environment}"
            pipeline = get_arg_value(params, "--pipeline")
            assert pipeline is None

    validate_bundle(
        metadata=metadata,
        env=config.default_env,
        required_files=[
            f"target.{config.default_env}.jobs.{metadata.package_name}_ds.yml",
            f"target.{config.default_env}.volumes.{metadata.package_name}_volume.yml",
        ],
        task_validator=task_validator,
    )


def test_databricks_bundle_with_no_nodes(metadata, cli_runner):
    """Test the `bundle` command"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)

    # Act
    result = cli_runner.invoke(
        command,
        [
            "--env",
            config.default_env,
            "--pipeline",
            "non-existing-pipeline",
            "--overwrite",
        ],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 1, (result.exit_code, result.stdout, result.exception)
    assert isinstance(result.exception, KeyError)
    exception = bytes(str(result.exception), "utf-8").decode("unicode_escape")
    expected = "Pipeline 'non-existing-pipeline' not found."
    assert expected in result.exception.args[0], (
        f"Expected exception message: {expected} not found in {exception}"
    )
