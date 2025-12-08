from kedro_databricks import __version__ as expected
from kedro_databricks.commands.version import command
from kedro_databricks.plugin import commands


def test_version(cli_runner):
    """Test the version command."""
    result = cli_runner.invoke(commands, ["databricks", "version"])
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert expected in result.stdout, (
        f"Expected version {expected} not found in output: {result.stdout}"
    )


def test_version_command(cli_runner):
    """Test the version command directly."""
    result = cli_runner.invoke(command, [])
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert expected in result.stdout, (
        f"Expected version {expected} not found in output: {result.stdout}"
    )
