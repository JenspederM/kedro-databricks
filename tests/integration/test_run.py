from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

from dotenv import load_dotenv
from kedro.framework.cli.starters import create_cli as kedro_cli
from kedro.framework.startup import bootstrap_project
from pytest import fixture

from kedro_databricks.constants import DEFAULT_TARGET
from kedro_databricks.plugin import commands

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


@fixture(scope="session")
def custom_username():
    custom_username = os.getenv("CUSTOM_USERNAME")
    return custom_username


PROJECT_NAME = "databricks-iris"


@fixture
def databricks_iris_starter(cli_runner):
    project_path = Path().cwd() / PROJECT_NAME
    if project_path.exists():
        shutil.rmtree(project_path)
    cli_runner.invoke(
        # Supply name, tools, and example to skip interactive prompts
        kedro_cli,
        ["new", "-v", "--starter", "databricks-iris", "--name", PROJECT_NAME],
    )
    assert project_path.exists(), "Project path not created"
    assert project_path.is_dir(), "Project path is not a directory"
    # This is needed to ensure that the project is created in the correct directory
    # and not in the current working directory.
    os.chdir(project_path)
    return project_path


@fixture
def iris_meta(databricks_iris_starter):
    # cwd() depends on ^ the isolated filesystem, created by CliRunner()
    project_path = databricks_iris_starter.resolve()
    metadata = bootstrap_project(project_path)
    return metadata


def test_run(cli_runner, iris_meta):
    """Test the run command."""
    project_path = iris_meta.project_path
    provider = "azure"
    command = ["databricks", "init", "--provider", provider]
    result = cli_runner.invoke(commands, command, obj=iris_meta)  # noqa: F821
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert (project_path / "databricks.yml").exists(), "Databricks config not created"
    command = ["databricks", "bundle", "--env", DEFAULT_TARGET]
    result = cli_runner.invoke(commands, command, obj=iris_meta)
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert (project_path / "resources").exists(), "Resources directory not created"
    deploy_cmd = ["databricks", "deploy", "--bundle"]
    result = cli_runner.invoke(commands, deploy_cmd, obj=iris_meta)
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    command = ["databricks", "run", iris_meta.package_name]
    result = cli_runner.invoke(commands, command, obj=iris_meta)
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    command = ["databricks", "destroy", "--auto-approve"]
    result = cli_runner.invoke(commands, command, obj=iris_meta)
    assert result.exit_code == 0, (result.exit_code, result.stdout)
