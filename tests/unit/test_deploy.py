from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from subprocess import CompletedProcess
from typing import cast

import pytest
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.cli.deploy import (
    _build_project,
    _check_deployment_complete,
    _get_arg_value,
    _validate_project,
)
from kedro_databricks.cli.deploy.get_deployed_resources import (
    _get_username,
    _is_valid_job,
)


@dataclass
class MockResult:
    stdout: list[str]
    stderr: list[str]


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


def test_deploy_validate_project(metadata):
    with pytest.raises(
        FileNotFoundError, match="Databricks configuration file does not exist."
    ):
        _validate_project(metadata)


def test_deploy_non_existing_path(metadata):
    with pytest.raises(FileNotFoundError, match="Project path.*does not exist"):
        _validate_project(
            cast(
                ProjectMetadata,  # noqa: F821
                MetadataMock(
                    "/tmp/non_existent_path" + str(os.getpid()), "non_existent_project"
                ),
            )
        )


def test_deploy_valid_project(metadata):
    Path(metadata.project_path / "databricks.yml").write_text("")
    _validate_project(metadata)


def test_deploy_build_project(metadata):
    result = _build_project(metadata)
    assert result.returncode == 0, (result.returncode, result.stdout)


@pytest.mark.parametrize(
    "args, arg, expected",
    [
        (["--env", "local"], "--env", "local"),
        (["--env", "dev"], "--env", "dev"),
        (["--env", "prod"], "--env", "prod"),
        (["--target", "local"], "--target", "local"),
        (["--target", "dev"], "--target", "dev"),
        (["--target", "prod"], "--target", "prod"),
        (["my-program", "--arg1", "value1", "--arg2", "value2"], "--arg1", "value1"),
        (["my-program", "--arg1", "value1", "--arg2", "value2"], "--arg2", "value2"),
        (["my-program", "--arg1=value1"], "--arg1", "value1"),
    ],
)
def test_get_arg_value(args, arg, expected):
    """Test the function to get the value of a specific argument from a list of arguments."""
    result = _get_arg_value(args, arg)
    assert result == expected, f"Expected {expected}, but got {result}"


@pytest.mark.parametrize(
    "result, expected",
    [
        (MockResult([], []), False),
        (MockResult(["Deployment complete!"], []), True),
        (MockResult([], ["Deployment complete!"]), True),
    ],
)
def test_check_deployment_complete(result, expected):
    assert _check_deployment_complete(cast(CompletedProcess, result)) == expected


@pytest.mark.parametrize(
    "pipelines, job_name, expected",
    [
        ({"__default__": {}}, "fake_project", True),
        ({"test_job": {}}, "fake_project_test_job", True),
        ({"test_job": {}}, "test_job", False),
    ],
)
def test_is_valid_job(metadata, pipelines, job_name, expected):
    is_valid = _is_valid_job(metadata, pipelines, job_name)
    assert is_valid is expected, "Job should be valid"


@pytest.mark.parametrize(
    "username, raises, expected",
    [
        ("user@email", False, "user"),
        ("test", False, "test"),
        ("test@", False, "test"),
        (None, True, None),
    ],
)
def test_get_username(username, raises, expected):
    username = _get_username("user@email.com")
    assert username == "user", f"Expected 'user', but got {username}"
