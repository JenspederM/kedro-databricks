from __future__ import annotations

from pathlib import Path

import pytest

from kedro_databricks.init import InitController
from kedro_databricks.utils.create_target_configs import (
    _get_bundle_name,
    _get_targets,
    _read_databricks_config,
)


def test_update_gitignore(metadata):
    controller = InitController(metadata)
    controller.update_gitignore()
    gitignore_path = metadata.project_path / ".gitignore"
    assert gitignore_path.exists(), "Gitignore not written"
    with open(gitignore_path) as f:
        assert ".databricks" in f.read(), "Databricks not in gitignore"


def test_update_gitignore_does_not_exist(metadata):
    if (metadata.project_path / ".gitignore").exists():
        (metadata.project_path / ".gitignore").unlink()
    controller = InitController(metadata)
    controller.update_gitignore()
    gitignore_path = metadata.project_path / ".gitignore"
    assert gitignore_path.exists(), "Gitignore not written"
    with open(gitignore_path) as f:
        assert ".databricks" in f.read(), "Databricks not in gitignore"


def test_read_databricks_config_does_not_exist(metadata):
    if (metadata.project_path / "databricks.yml").exists():
        (metadata.project_path / "databricks.yml").unlink()
    with pytest.raises(FileNotFoundError):
        _read_databricks_config(metadata.project_path)


def test_read_bundle_name_does_not_exist(metadata):
    with pytest.raises(ValueError):
        _get_bundle_name({})


def test_get_targets_does_not_exist(metadata):
    with pytest.raises(ValueError):
        _get_targets({})


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
