from __future__ import annotations

import os
from pathlib import Path

import pytest
from kedro_databricks.deploy import (
    build_project,
    go_to_project,
    validate_databricks_config,
)


class MetadataMock:
    def __init__(self, path: str, name: str):
        self.project_path = Path(path)
        self.project_name = name
        self.package_name = name
        self.source_dir = "src"
        self.env = "local"
        self.config_file = "conf/base"
        self.project_version = "0.16.0"
        self.project_description = "Test Project Description"
        self.project_author = "Test Author"
        self.project_author_email = "author@email.com"


def test_deploy_go_to_project(metadata):
    project_path = go_to_project(metadata)
    assert os.getcwd() == str(project_path), "Failed to change to project directory"
    with pytest.raises(FileNotFoundError):
        go_to_project(
            MetadataMock(
                "/tmp/non_existent_path" + str(os.getpid()), "non_existent_project"
            )
        )


def test_deploy_validate_databricks_config(metadata):
    project_path = go_to_project(metadata)
    (project_path / "databricks.yml").unlink(missing_ok=True)
    with pytest.raises(FileNotFoundError):
        validate_databricks_config(metadata)
    with open(project_path / "databricks.yml", "w") as f:
        f.write("")
    validate_databricks_config(metadata)


def test_deploy_build_project(metadata):
    result = build_project(metadata, "Test Build Project")
    assert result.returncode == 0, (result.returncode, result.stdout)
