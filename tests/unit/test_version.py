import tomli

from kedro_databricks.plugin import commands
from tests import PROJECT_ROOT


def test_version(cli_runner):
    """Test the version command."""
    with open(PROJECT_ROOT / "pyproject.toml", "rb") as f:
        project = tomli.load(f)
    result = cli_runner.invoke(commands, ["databricks", "version"])
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert (
        project["project"]["version"] in result.stdout
    ), f"Expected version {project['project']['version']} not found in output: {result.stdout}"

    pass
