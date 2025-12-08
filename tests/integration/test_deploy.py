import shutil

from click.testing import Result

from kedro_databricks.commands.deploy import command
from kedro_databricks.constants import DEFAULT_ENV
from tests.utils import (
    bundle_project,
    destroy_project,
    init_project,
    reset_init,
    reset_project,
)


def _validate_deploy(
    result: Result,
):
    assert result.exit_code == 0 and "Deployment complete!\n" in result.stdout, (
        result.exit_code,
        result.stdout,
        result.exception,
    )


def test_deploy_fail(cli_runner, metadata):
    """Test the `deploy` command"""
    reset_init(metadata)
    result = cli_runner.invoke(
        command,
        [],
        obj=metadata,
    )
    assert result.exit_code == 1, (result.exit_code, result.stdout, result.exception)


def test_bundled_deploy(metadata, cli_runner):
    """Test the `deploy` command"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)

    # Act
    result = cli_runner.invoke(
        command,
        [
            "--bundle",
        ],
        obj=metadata,
    )

    # Assert
    _validate_deploy(result=result)

    # Cleanup
    destroy_project(metadata, cli_runner)


def test_deploy(metadata, cli_runner):
    """Test the `deploy` command"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)
    bundle_project(metadata, cli_runner)

    # Act
    result = cli_runner.invoke(
        command,
        [],
        obj=metadata,
    )

    # Assert
    _validate_deploy(result=result)

    # Cleanup
    destroy_project(metadata, cli_runner)


def test_deploy_prod(metadata, cli_runner):
    """Test the `deploy` command"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)
    bundle_project(metadata, cli_runner)

    # Act
    result = cli_runner.invoke(
        command,
        [
            "--env",
            "prod",
        ],
        obj=metadata,
    )

    # Assert
    _validate_deploy(result=result)

    # Cleanup
    destroy_project(metadata, cli_runner)


def test_deploy_with_conf(metadata, cli_runner):
    """Test the `deploy` command"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)
    bundle_project(metadata, cli_runner)
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
            "--bundle",
            "--conf-source",
            CONF_KEY,
        ],
        obj=metadata,
    )

    # Assert
    _validate_deploy(result=result)

    # Revert
    settings.write_text(original_settings)
    destroy_project(metadata, cli_runner)
