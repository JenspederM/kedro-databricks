from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import yaml

from kedro_databricks.cli.init import (
    _prepare_template,
    _update_gitignore,
    _validate_inputs,
    _write_databricks_run_script,
    init,
)
from kedro_databricks.cli.init.create_target_configs import (
    DatabricksTarget,
    _substitute_file_path,
    create_target_configs,
)
from kedro_databricks.constants import NODE_TYPE_MAP


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


def test_bundle_init_already_exists(metadata):
    with open(metadata.project_path / "databricks.yml", "w") as f:
        f.write("test")
    with pytest.raises(RuntimeError, match="databricks.yml already exists"):
        init(metadata, "azure", "test")
    with open(metadata.project_path / "databricks.yml") as f:
        assert f.read() == "test", "Databricks config overwritten"


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
    monkeypatch.setattr(
        DatabricksTarget,
        "_get_metadata",
        lambda *args, **kwargs: {
            "workspace": {
                "file_path": "/Workspace/<your-volume-name>/develop_eggs/data/01_raw/file.csv",
            }
        },  # noqa: E501
    )
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
                            "host": "https://<your-volume-name>.databricks.com",
                        },
                    }
                },
            },
            f,
        )
    create_target_configs(
        metadata, NODE_TYPE_MAP.get("azure", ""), "test", single_user_default=False
    )


def test_validate_inputs_unknown_provider(metadata):
    with pytest.raises(ValueError, match="Invalid provider 'unknown'"):
        _validate_inputs(metadata, "unknown")


def test_validate_inputs(metadata):
    if (metadata.project_path / "databricks.yml").exists():
        (metadata.project_path / "databricks.yml").unlink()
    config_path, node_type_id = _validate_inputs(metadata, "azure")
    assert config_path == metadata.project_path / "databricks.yml"
    assert node_type_id == NODE_TYPE_MAP["azure"]


def test_validate_inputs_config_already_exists(metadata):
    if not (metadata.project_path / "databricks.yml").exists():
        (metadata.project_path / "databricks.yml").write_text("test")
    with pytest.raises(RuntimeError, match="databricks.yml already exists"):
        _validate_inputs(metadata, "azure")


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
