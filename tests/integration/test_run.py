from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner
from kedro.framework.cli.starters import create_cli as kedro_cli
from kedro.framework.startup import bootstrap_project

from kedro_databricks.commands.run import command as run_command
from tests.utils import (
    bundle_project,
    deploy_project,
    destroy_project,
    init_project,
    reset_project,
)

PROJECT_NAME = "databricks-iris"


@pytest.fixture
def run_cli_runner():
    runner = CliRunner()
    with runner.isolated_filesystem():
        yield runner


@pytest.fixture
def databricks_iris_starter(run_cli_runner):
    project_path = Path().cwd() / PROJECT_NAME
    if project_path.exists():
        shutil.rmtree(project_path)
    run_cli_runner.invoke(
        kedro_cli,
        ["new", "-v", "--starter", "databricks-iris", "--name", PROJECT_NAME],
    )
    assert project_path.exists(), "Project path not created"
    assert project_path.is_dir(), "Project path is not a directory"
    os.chdir(project_path)
    return project_path


@pytest.fixture
def iris_meta(databricks_iris_starter):
    project_path = databricks_iris_starter.resolve()
    metadata = bootstrap_project(project_path)
    return metadata


def test_run(run_cli_runner, iris_meta):
    """Test the run command."""
    # Arrange
    reset_project(iris_meta)
    init_project(iris_meta, run_cli_runner)
    bundle_project(iris_meta, run_cli_runner)
    deploy_project(iris_meta, run_cli_runner)

    # Act
    result = run_cli_runner.invoke(run_command, [], obj=iris_meta)
    # Assert
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)

    # Cleanup
    destroy_project(iris_meta, run_cli_runner)
