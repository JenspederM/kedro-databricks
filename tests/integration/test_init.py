from kedro_databricks.cli.init.create_target_configs import (
    _get_bundle_name,
    _get_targets,
    _read_databricks_config,
)
from kedro_databricks.plugin import commands
from tests.utils import reset_init


def test_init_arg(cli_runner, metadata):
    reset_init(metadata)
    command = ["databricks", "init"]
    result = cli_runner.invoke(commands, command, obj=metadata)
    assert result.exit_code == 0, (
        result.exit_code,
        result.stdout,
        result.exception,
    )
    files = [f"{f.parent.name}/{f.name}" for f in metadata.project_path.rglob("*")]
    assert len(files) > 0, "Found no files in the directory."

    config_path = metadata.project_path / "databricks.yml"
    assert config_path.exists(), f"Configuration at {config_path} does not exist"

    databricks_config = _read_databricks_config(metadata.project_path)
    assert databricks_config is not None, "Databricks config not read"
    name = _get_bundle_name(databricks_config)
    assert name == metadata.package_name, f"Bundle name not set: {name}"

    targets = _get_targets(databricks_config)
    for target in targets:
        override_path = metadata.project_path / "conf" / target / "databricks.yml"
        assert override_path.exists(), (
            f"Resource Overrides at {override_path} does not exist"
        )
