from kedro_databricks.plugin import commands
from tests.utils import reset_init


def test_databricks_init(kedro_project, cli_runner, metadata):
    """Test the `init` command"""
    reset_init(metadata)
    command = ["databricks", "init"]
    result = cli_runner.invoke(commands, command, obj=metadata)

    files = [f"{f.parent.name}/{f.name}" for f in kedro_project.rglob("*")]
    assert len(files) > 0, "Found no files in the directory."

    config_path = kedro_project / "databricks.yml"
    override_path = kedro_project / "conf" / "base" / "databricks.yml"
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert config_path.exists(), f"Configuration at {config_path} does not exist"
    assert (
        override_path.exists()
    ), f"Resource Overrides at {override_path} does not exist"

    command = ["databricks", "init"]
    result = cli_runner.invoke(commands, command, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)
