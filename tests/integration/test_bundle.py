import shutil

from kedro_databricks.commands.bundle import command
from kedro_databricks.constants import DEFAULT_CONFIG_KEY, DEFAULT_ENV
from kedro_databricks.utilities.common import get_arg_value
from tests.utils import init_project, reset_project, validate_bundle


def test_databricks_bundle_fail(metadata, cli_runner):
    """Test the `bundle` command failure case"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)

    # Act
    result = cli_runner.invoke(
        command,
        ["--default-key", "_deault"],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 1, (result.exit_code, result.stdout, result.exception)


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
            DEFAULT_ENV,
        ],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)

    def task_validator(tasks):
        assert len(tasks) == 8
        for i, task in enumerate(tasks):
            assert task.get("task_key") in (f"node{i}", f"ns_{i}_node_{i}_1")
            assert task.get("environment_key") == DEFAULT_CONFIG_KEY
            params = task.get("python_wheel_task").get("parameters")
            assert params is not None
            env = get_arg_value(params, "--env")
            assert env is not None
            assert env == "${var.environment}"

    validate_bundle(
        metadata=metadata,
        env=DEFAULT_ENV,
        required_files=[
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}.yml",
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}_ds.yml",
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}_namespaced_pipeline.yml",
            f"target.{DEFAULT_ENV}.volumes.{metadata.package_name}_volume.yml",
        ],
        task_validator=task_validator,
    )


def test_databricks_bundle_with_conf(metadata, cli_runner):
    """Test the `bundle` command"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)
    CONF_KEY = "custom_conf"
    default_path = metadata.project_path / "conf" / DEFAULT_ENV / "databricks.yml"
    override_path = metadata.project_path / CONF_KEY / DEFAULT_ENV / "databricks.yml"
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
            DEFAULT_ENV,
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
            assert task.get("environment_key") == DEFAULT_CONFIG_KEY
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
        env=DEFAULT_ENV,
        required_files=[
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}.yml",
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}_ds.yml",
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}_namespaced_pipeline.yml",
            f"target.{DEFAULT_ENV}.volumes.{metadata.package_name}_volume.yml",
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
    shutil.rmtree(metadata.project_path / "conf" / DEFAULT_ENV)

    # Act
    result = cli_runner.invoke(
        command,
        [
            "--env",
            DEFAULT_ENV,
        ],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 1, (result.exit_code, result.stdout, result.exception)
    assert isinstance(result.exception, FileNotFoundError)
    exception = bytes(str(result.exception), "utf-8").decode("unicode_escape")
    assert "Databricks configuration for environment" in exception
    assert "not found" in exception


def test_databricks_bundle_empty_overrides(metadata, cli_runner):
    """Test the `bundle` command"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)
    (metadata.project_path / "conf" / DEFAULT_ENV / "databricks.yml").write_text("")

    # Act
    result = cli_runner.invoke(
        command,
        [
            "--env",
            DEFAULT_ENV,
        ],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 1, (result.exit_code, result.stdout, result.exception)
    assert isinstance(result.exception, KeyError)
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
            DEFAULT_ENV,
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
            assert task.get("environment_key") == DEFAULT_CONFIG_KEY
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
        env=DEFAULT_ENV,
        required_files=[
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}.yml",
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}_ds.yml",
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}_namespaced_pipeline.yml",
            f"target.{DEFAULT_ENV}.volumes.{metadata.package_name}_volume.yml",
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
            DEFAULT_ENV,
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
            assert task.get("environment_key") == DEFAULT_CONFIG_KEY
            params = task.get("python_wheel_task").get("parameters")
            assert params is not None
            env = get_arg_value(params, "--env")
            assert env is not None
            assert env == "${var.environment}"
            pipeline = get_arg_value(params, "--pipeline")
            assert pipeline is None

    validate_bundle(
        metadata=metadata,
        env=DEFAULT_ENV,
        required_files=[
            f"target.{DEFAULT_ENV}.jobs.{metadata.package_name}_ds.yml",
            f"target.{DEFAULT_ENV}.volumes.{metadata.package_name}_volume.yml",
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
            DEFAULT_ENV,
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
