from __future__ import annotations

import os
from pathlib import Path

import pytest

from kedro_databricks.deploy import DeployController


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
    controller = DeployController(metadata)
    project_path = controller.go_to_project()
    assert os.getcwd() == str(project_path), "Failed to change to project directory"
    with pytest.raises(FileNotFoundError):
        controller = DeployController(
            MetadataMock(
                "/tmp/non_existent_path" + str(os.getpid()), "non_existent_project"
            )
        )
        controller.go_to_project()


def test_deploy_validate_databricks_config(metadata):
    controller = DeployController(metadata)
    project_path = controller.go_to_project()
    (project_path / "databricks.yml").unlink(missing_ok=True)
    with pytest.raises(FileNotFoundError):
        controller.validate_databricks_config()
    with open(project_path / "databricks.yml", "w") as f:
        f.write("")
    controller.validate_databricks_config()


def test_deploy_build_project(metadata):
    controller = DeployController(metadata)
    result = controller.build_project()
    assert result.returncode == 0, (result.returncode, result.stdout)
