from __future__ import annotations

import shutil
from pathlib import Path

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


def test_run():
    """Test the run command."""
    PROJECT_NAME = "databricks-iris"
    runner = CliRunner()
    with runner.isolated_filesystem():
        project_path = Path().cwd() / PROJECT_NAME
        if project_path.exists():
            shutil.rmtree(project_path)
        runner.invoke(
            kedro_cli,
            ["new", "-v", "--starter", "databricks-iris", "--name", PROJECT_NAME],
        )
        assert project_path.exists(), "Project path not created"
        assert project_path.is_dir(), "Project path is not a directory"
        metadata = bootstrap_project(project_path)
        # Arrange
        reset_project(metadata)
        init_project(metadata, runner)
        bundle_project(metadata, runner)
        deploy_project(metadata, runner)

        # Act
        result = runner.invoke(run_command, [], obj=metadata)
        # Assert
        assert result.exit_code == 0, (
            result.exit_code,
            result.stdout,
            result.exception,
        )

        # Cleanup
        destroy_project(metadata, runner)
