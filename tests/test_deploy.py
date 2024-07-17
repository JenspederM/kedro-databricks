import os

import pytest


def test_deploy_go_to_project(kedro_project):
    from kedro_databricks.deploy import _go_to_project

    project_path = _go_to_project(kedro_project)
    assert os.getcwd() == str(project_path), "Failed to change to project directory"

    with pytest.raises(FileNotFoundError):
        _go_to_project("/tmp/non_existent_path" + str(os.getpid()))


def test_deploy_validate_databricks_config(kedro_project, cli_runner, metadata):
    from kedro_databricks.deploy import _go_to_project, _validate_databricks_config

    project_path = _go_to_project(kedro_project)

    with pytest.raises(FileNotFoundError):
        _validate_databricks_config(project_path)

    with open(project_path / "databricks.yml", "w") as f:
        f.write("")

    _validate_databricks_config(project_path)


def test_deploy_build_project(metadata):
    from kedro_databricks.deploy import _build_project

    result = _build_project(metadata, "Test Build Project")
    assert result.returncode == 0, (result.returncode, result.stdout)
