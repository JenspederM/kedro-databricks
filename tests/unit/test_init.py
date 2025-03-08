from __future__ import annotations

from pathlib import Path

import pytest

from kedro_databricks.init import InitController
from kedro_databricks.utils.create_target_configs import (
    _get_bundle_name,
    _get_targets,
    _read_databricks_config,
    create_target_configs,
)


def test_read_databricks_config(metadata):
    if (metadata.project_path / "databricks.yml").exists():
        (metadata.project_path / "databricks.yml").unlink()
    controller = InitController(metadata)
    controller.bundle_init([])
    databricks_config = _read_databricks_config(metadata.project_path)
    files = [f.name for f in metadata.project_path.iterdir()]
    assert (
        "databricks.yml" in files
    ), f"Databricks config not created - found files: {files}"
    assert databricks_config is not None, "Databricks config not read"


def test_read_databricks_config_does_not_exist(metadata):
    if (metadata.project_path / "databricks.yml").exists():
        (metadata.project_path / "databricks.yml").unlink()
    with pytest.raises(FileNotFoundError):
        _read_databricks_config(metadata.project_path)


def test_read_bundle_name(metadata):
    if (metadata.project_path / "databricks.yml").exists():
        (metadata.project_path / "databricks.yml").unlink()
    controller = InitController(metadata)
    controller.bundle_init([])
    databricks_config = _read_databricks_config(metadata.project_path)
    bundle_name = _get_bundle_name(databricks_config)
    assert bundle_name == metadata.package_name, "Bundle name not read"


def test_read_bundle_name_does_not_exist(metadata):
    with pytest.raises(ValueError):
        _get_bundle_name({})


def test_get_targets(metadata):
    if (metadata.project_path / "databricks.yml").exists():
        (metadata.project_path / "databricks.yml").unlink()
    controller = InitController(metadata)
    controller.bundle_init([])
    databricks_config = _read_databricks_config(metadata.project_path)
    targets = _get_targets(databricks_config)
    assert targets is not None, "Targets not read"


def test_get_targets_does_not_exist(metadata):
    with pytest.raises(ValueError):
        _get_targets({})


def test_create_target_configs(metadata):
    if (metadata.project_path / "databricks.yml").exists():
        (metadata.project_path / "databricks.yml").unlink()
    if (metadata.project_path / "conf" / "base" / "databricks.yml").exists():
        (metadata.project_path / "conf" / "base" / "databricks.yml").rmdir()
    controller = InitController(metadata)
    controller.bundle_init([])
    create_target_configs(metadata, "test", "test")
    databricks_config = _read_databricks_config(metadata.project_path)

    targets = _get_targets(databricks_config)
    for target in targets:
        target_path = metadata.project_path / "conf" / target
        assert target_path.exists(), f"Target config not created: {target_path}"
        assert target_path.is_dir(), f"Target config is not a directory: {target_path}"


def test_bundle_init_already_exists(metadata):
    controller = InitController(metadata)
    with open(metadata.project_path / "databricks.yml", "w") as f:
        f.write("test")
    controller.bundle_init([])
    with open(metadata.project_path / "databricks.yml") as f:
        assert f.read() == "test", "Databricks config overwritten"


def test_write_databricks_run_script(metadata):
    controller = InitController(metadata)
    controller.write_databricks_run_script()
    run_script_path = (
        Path(controller.project_path)
        / "src"
        / controller.package_name
        / "databricks_run.py"
    )
    assert run_script_path.exists(), "Databricks run script not written"
