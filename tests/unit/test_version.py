import tomlkit

from kedro_databricks.plugin import commands
from tests import PROJECT_ROOT


def test_version(cli_runner):
    """Test the version command."""
    with open(PROJECT_ROOT / "pyproject.toml", "rb") as f:
        project = tomlkit.load(f)
    result = cli_runner.invoke(commands, ["databricks", "version"])
    expected = project["project"]["version"]  # type: ignore
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert (
        expected in result.stdout
    ), f"Expected version {expected} not found in output: {result.stdout}"
