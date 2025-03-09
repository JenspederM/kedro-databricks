from pathlib import Path

import pytest
import yaml

from kedro_databricks.init import InitController
from kedro_databricks.plugin import commands
from kedro_databricks.utils.create_target_configs import (
    _get_bundle_name,
    _get_targets,
    _read_databricks_config,
    create_target_configs,
)
from tests.utils import reset_init


def test_read_databricks_config(metadata):
    reset_init(metadata)
    controller = InitController(metadata)
    controller.bundle_init([])
    databricks_config = _read_databricks_config(metadata.project_path)
    files = [f.name for f in metadata.project_path.iterdir()]
    assert (
        "databricks.yml" in files
    ), f"Databricks config not created - found files: {files}"
    assert databricks_config is not None, "Databricks config not read"


def test_read_bundle_name(metadata):
    reset_init(metadata)
    controller = InitController(metadata)
    controller.bundle_init([])
    databricks_config = _read_databricks_config(metadata.project_path)
    bundle_name = _get_bundle_name(databricks_config)
    assert bundle_name == metadata.package_name, "Bundle name not read"


def test_get_targets(metadata):
    reset_init(metadata)
    controller = InitController(metadata)
    controller.bundle_init([])
    databricks_config = _read_databricks_config(metadata.project_path)
    targets = _get_targets(databricks_config)
    assert targets is not None, "Targets not read"


def test_create_target_configs(metadata):
    reset_init(metadata)
    controller = InitController(metadata)
    controller.bundle_init([])
    create_target_configs(metadata, "test", "test")
    databricks_config = _read_databricks_config(metadata.project_path)

    targets = _get_targets(databricks_config)
    for target in targets:
        target_path = metadata.project_path / "conf" / target
        assert target_path.exists(), f"Target config not created: {target_path}"
        assert target_path.is_dir(), f"Target config is not a directory: {target_path}"


def test_unknown_provider(cli_runner, metadata):
    reset_init(metadata)
    command = ["databricks", "init", "--provider", "unknown"]
    result = cli_runner.invoke(commands, command, obj=metadata)
    assert result.exit_code == 1, (result.exit_code, result.stdout)


def test_databricks_init(kedro_project, cli_runner, metadata):
    """Test the `init` command"""
    reset_init(metadata)
    command = ["databricks", "init", "--provider", "azure"]
    result = cli_runner.invoke(commands, command, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)

    files = [f"{f.parent.name}/{f.name}" for f in kedro_project.rglob("*")]
    assert len(files) > 0, "Found no files in the directory."

    config_path = kedro_project / "databricks.yml"
    assert config_path.exists(), f"Configuration at {config_path} does not exist"

    databricks_config = _read_databricks_config(kedro_project)
    targets = _get_targets(databricks_config)
    for target in targets:
        override_path = kedro_project / "conf" / target / "databricks.yml"
        assert (
            override_path.exists()
        ), f"Resource Overrides at {override_path} does not exist"

    command = ["databricks", "init"]
    result = cli_runner.invoke(commands, command, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)


def test_bundle_init(metadata):
    controller = InitController(metadata)
    reset_init(metadata)
    controller.bundle_init([])
    bundle_path = Path(metadata.project_path) / "databricks.yml"
    if not bundle_path.exists():
        files = [
            f.relative_to(metadata.project_path).as_posix()
            for f in metadata.project_path.iterdir()
        ]
        pytest.fail(
            "Bundle file not written - found files:\n\t{}".format("\n\t".join(files))
        )

    bundle = yaml.load(bundle_path.read_text(), Loader=yaml.FullLoader)
    assert (
        bundle is not None
    ), f"Bundle template failed to load - {bundle_path.read_text()}"
    assert bundle.get("bundle", {}).get("name") == metadata.package_name, bundle

    try:
        controller.bundle_init([])
    except Exception:
        pytest.fail("If a bundle file already exists, it should not be overwritten.")
