from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import yaml

from kedro_databricks.commands.init import (
    _create_target_configs,
    _prepare_template,
    _substitute_file_path,
    _update_gitignore,
    _write_databricks_run_script,
)
from kedro_databricks.constants import DEFAULT_CATALOG, DEFAULT_SCHEMA


def test_update_gitignore(metadata):
    _update_gitignore(metadata)
    gitignore_path = metadata.project_path / ".gitignore"
    assert gitignore_path.exists(), "Gitignore not written"
    with open(gitignore_path) as f:
        assert ".databricks" in f.read(), "Databricks not in gitignore"


def test_update_gitignore_does_not_exist(metadata):
    if (metadata.project_path / ".gitignore").exists():
        (metadata.project_path / ".gitignore").unlink()
    _update_gitignore(metadata)
    gitignore_path = metadata.project_path / ".gitignore"
    assert gitignore_path.exists(), "Gitignore not written"
    with open(gitignore_path) as f:
        assert ".databricks" in f.read(), ".databricks not in gitignore"


def test_write_databricks_run_script(metadata):
    _write_databricks_run_script(metadata)
    run_script_path = (
        Path(metadata.project_path)
        / "src"
        / metadata.package_name
        / "databricks_run.py"
    )
    assert run_script_path.exists(), "Databricks run script not written"


@pytest.mark.parametrize(
    ["actual", "expected"],
    [
        (
            "file_path: /dbfs/FileStore/develop_eggs/data/01_raw/file.csv",
            "file_path: ${_file_path}/data/01_raw/file.csv",
        ),
        (
            "file_path: /dbfs/develop_eggs/data/01_raw/file.csv",
            "file_path: ${_file_path}/data/01_raw/file.csv",
        ),
        (
            "file_path: /dbfs/FileStore/develop_eggs/data/01_raw/file.csv",
            "file_path: ${_file_path}/data/01_raw/file.csv",
        ),
        (
            "file_path: data/0_raw/file.csv",
            "file_path: ${_file_path}/data/0_raw/file.csv",
        ),
        (
            "file_path: data/012_raw/file.csv",
            "file_path: ${_file_path}/data/012_raw/file.csv",
        ),
        (
            "file_path: /custom/path/data/01_raw/file.csv",
            "file_path: ${_file_path}/data/01_raw/file.csv",
        ),
        ("data/01_raw/file.csv", "data/01_raw/file.csv"),
    ],
)
def test_substitute_file_path(actual, expected):
    result = _substitute_file_path(actual)
    assert result == expected, f"\n{result}\n{expected}"


def test_create_target_configs(metadata, monkeypatch):
    with open(metadata.project_path / "databricks.yml", "w") as f:
        yaml.safe_dump(
            {
                "bundle": {
                    "name": "develop_eggs",
                },
                "targets": {
                    "dev": {
                        "mode": "development",
                        "workspace": {
                            "host": "https://<your-volume-name>.databricks.com"
                        },
                    }
                },
            },
            f,
        )
    _create_target_configs(
        metadata,
        "test",
        DEFAULT_CATALOG,
        DEFAULT_SCHEMA,
        {"workspace": {"current_user": {"short_name": "test_user"}}},
    )


def test_prepare_template(metadata):
    assets_dir, template_params = _prepare_template(metadata)
    assert assets_dir.exists(), "Assets directory not created"
    params = template_params.read_text()
    try:
        conf = json.loads(params)
        assert conf["project_name"] == metadata.package_name, "Project name not set"
        assert conf["project_slug"] == metadata.package_name, "Package name not set"
    except Exception as e:
        raise ValueError(f"Failed to load template params: {e} - {params}")
    shutil.rmtree(assets_dir)
