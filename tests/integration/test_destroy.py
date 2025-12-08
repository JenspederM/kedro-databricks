from kedro_databricks.commands.destroy import command
from tests.utils import bundle_project, deploy_project, init_project, reset_project


def test_destroy(metadata, cli_runner):
    """Test the `destroy` command"""
    # Arrange
    reset_project(metadata)
    init_project(metadata, cli_runner)
    bundle_project(metadata, cli_runner)
    deploy_project(metadata, cli_runner)

    # Act
    result = cli_runner.invoke(
        command,
        ["--", "--auto-approve"],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)
