from kedro_databricks.commands.init import (
    _get_bundle_name,
    _get_targets,
    _read_databricks_config,
    command,
)
from tests.utils import reset_init


def test_bundle_init_already_exists(cli_runner, metadata):
    # Arrange
    reset_init(metadata)
    with open(metadata.project_path / "databricks.yml", "w") as f:
        f.write("")

    # Act
    result = cli_runner.invoke(
        command,
        [],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 1, (
        result.exit_code,
        result.stdout,
        result.exception,
    )
    assert "one or more files already exist: databricks.yml" in result.output
    with open(metadata.project_path / "databricks.yml") as f:
        assert f.read() == "", "Databricks config overwritten"


def test_init_arg(cli_runner, metadata):
    # Arrange
    reset_init(metadata)

    # Act
    result = cli_runner.invoke(
        command,
        [],
        obj=metadata,
    )

    # Assert
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
