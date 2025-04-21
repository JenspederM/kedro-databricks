from __future__ import annotations

import logging
import platform
import subprocess

import pytest

from kedro_databricks.constants import MINIMUM_DATABRICKS_VERSION
from kedro_databricks.utils import (
    Command,
    _version_to_str,
    assert_databricks_cli,
    get_bundle_name,
    get_targets,
    make_workflow_name,
    read_databricks_config,
)

log = logging.getLogger("test")


@pytest.mark.skipif(
    assert_databricks_cli(False) is not None,
    reason="Databricks CLI is not installed",
)
def test_assert_databricks_cli():
    assert_databricks_cli()


def test_read_databricks_config(metadata):
    if not (metadata.project_path / "databricks.yml").exists():
        (metadata.project_path / "databricks.yml").write_text(
            "bundle:\n  name: test_bundle\n"
        )
    read_databricks_config(metadata.project_path)


def test_read_databricks_config_does_not_exist(metadata):
    if (metadata.project_path / "databricks.yml").exists():
        (metadata.project_path / "databricks.yml").unlink()
    with pytest.raises(FileNotFoundError):
        read_databricks_config(metadata.project_path)


def test_read_bundle_name_does_not_exist():
    with pytest.raises(ValueError):
        get_bundle_name({})


def test_read_bundle_name():
    name = get_bundle_name({"bundle": {"name": "test_bundle"}})
    assert name == "test_bundle", name


def test_get_targets():
    targets = get_targets({"targets": {"test": "test"}})
    assert targets == {"test": "test"}, targets


def test_get_targets_does_not_exist():
    with pytest.raises(ValueError):
        get_targets({})


@pytest.mark.parametrize(
    ["version", "expected", "raises"],
    [
        ([1, 2, 3], "1.2.3", False),
        ([1, 2], "1.2", True),
        ([1], "1", True),
        ([1, 2, 3, 4], "1.2.3.4", True),
    ],
)
def test_version_to_str(version, expected, raises):
    if raises is True:
        with pytest.raises(ValueError):
            _version_to_str(version)
    else:
        result = _version_to_str(version)
        assert result == expected, f"Expected {expected}, but got {result}"


def test_fail_with_python_databricks_cli(monkeypatch):
    subprocess.run(["uv", "pip", "install", "databricks-cli"], check=True)
    monkeypatch.setenv("DATABRICKS_CLI_DO_NOT_EXECUTE_NEWER_VERSION", "1")
    with pytest.raises(
        RuntimeError,
        match=f"this script requires at least {'.'.join(map(str, MINIMUM_DATABRICKS_VERSION))}",
    ):
        assert_databricks_cli()
    subprocess.run(["uv", "pip", "uninstall", "databricks-cli"], check=True)


def test_fail_with_python_databricks_cli_soft(monkeypatch):
    subprocess.run(["uv", "pip", "install", "databricks-cli"], check=True)
    monkeypatch.setenv("DATABRICKS_CLI_DO_NOT_EXECUTE_NEWER_VERSION", "1")
    err = assert_databricks_cli(False)
    assert err is not None
    subprocess.run(["uv", "pip", "uninstall", "databricks-cli"], check=True)


OS = platform.uname().system.lower()


@pytest.mark.parametrize(
    ["cmd", "result_code", "warn", "raises"],
    [
        (["ls", "."], 0, False, False),
        (["ls", "non_existent_file"], 2, False, True),
        (["ls", "non_existent_file"], 1 if OS == "darwin" else 2, True, False),
        (["ls", "non_existent_file"], 2, False, True),
        (["ls", "non_existent_file"], 1 if OS == "darwin" else 2, True, False),
    ],
)
def test_command(cmd, result_code, warn, raises):
    if raises and not warn:
        with pytest.raises(Exception):
            result = Command(cmd, log=log).run()
            assert isinstance(result, subprocess.CompletedProcess)
            assert result.returncode == result_code, result
    else:
        command = Command(cmd, log=log, warn=warn)
        assert repr(command) == f"Command({cmd})", repr(command)
        result = command.run()
        assert isinstance(result, subprocess.CompletedProcess)
        assert result.returncode == result_code, result


@pytest.mark.parametrize(
    ["package_name", "pipeline_name", "expected"],
    [
        ("package", "__default__", "package"),
        ("package", "pipeline", "package_pipeline"),
    ],
)
def test_make_workflow_name(package_name, pipeline_name, expected):
    workflow_name = make_workflow_name(package_name, pipeline_name)
    assert workflow_name == expected, workflow_name
