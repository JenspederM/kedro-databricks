from __future__ import annotations

import logging
import os
import shutil
import subprocess

import pytest

from kedro_databricks.utils.common import (
    KEDRO_VERSION,
    Command,
    _is_null_or_empty,
    get_entry_point,
    make_workflow_name,
    remove_nulls,
    require_databricks_run_script,
    sort_dict,
)
from kedro_databricks.utils.create_target_configs import _substitute_file_path
from kedro_databricks.utils.has_databricks import has_databricks_cli
from kedro_databricks.utils.override_resources import (
    _get_lookup_key,
    _override_workflow,
    _update_list_by_key,
    override_resources,
)


@pytest.mark.skipif(
    shutil.which("databricks") is None, reason="Databricks CLI is not installed"
)
def test_has_databricks_cli():
    logging.basicConfig(level=logging.INFO)
    assert has_databricks_cli(), "Databricks CLI is not installed"


def test_fail_with_python_databricks_cli():
    subprocess.run(["uv", "pip", "install", "databricks-cli"], check=True)
    with pytest.raises(ValueError):
        os.environ["DATABRICKS_CLI_DO_NOT_EXECUTE_NEWER_VERSION"] = "1"
        has_databricks_cli()
    subprocess.run(["uv", "pip", "uninstall", "databricks-cli"], check=True)


def test_unknown_lookup_key():
    with pytest.raises(ValueError):
        _get_lookup_key("unknown")


def test_workflow_not_dict():
    with pytest.raises(ValueError):
        override_resources(
            {"resources": {"jobs": {"workflow": "not_dict"}}}, {}, "default"
        )


def test_overrides_not_dict():
    with pytest.raises(ValueError):
        override_resources(
            {"resources": {"jobs": {"workflow": {}}}},
            [],  # type: ignore
            "default",
        )


def test_substitute_file_path():
    tests = [
        (
            "file_path: /dbfs/FileStore/develop_eggs/data/01_raw/file.csv",
            "file_path: file://${_file_path}/data/01_raw/file.csv",
        ),
        (
            "file_path: /dbfs/develop_eggs/data/01_raw/file.csv",
            "file_path: file://${_file_path}/data/01_raw/file.csv",
        ),
        (
            "file_path: /dbfs/FileStore/develop_eggs/data/01_raw/file.csv",
            "file_path: file://${_file_path}/data/01_raw/file.csv",
        ),
        (
            "file_path: data/0_raw/file.csv",
            "file_path: file://${_file_path}/data/0_raw/file.csv",
        ),
        (
            "file_path: data/012_raw/file.csv",
            "file_path: file://${_file_path}/data/012_raw/file.csv",
        ),
        (
            "file_path: /custom/path/data/01_raw/file.csv",
            "file_path: file://${_file_path}/data/01_raw/file.csv",
        ),
        ("data/01_raw/file.csv", "data/01_raw/file.csv"),
    ]
    for file_path, expected in tests:
        result = _substitute_file_path(file_path)
        assert result == expected, f"\n{result}\n{expected}"


def test_command_fail_default():
    with pytest.raises(Exception) as e:
        cmd = ["ls", "non_existent_file"]
        Command(cmd).run()
        assert "Failed to run command" in str(
            e.value
        ), f"Failed to raise exception: {cmd}"
        raise Exception(e)


def test_command_fail_custom():
    with pytest.raises(Exception) as e:
        cmd = ["ls", "non_existent_file"]
        msg = "Custom message"
        Command(cmd, msg=msg).run()
        assert "Custom message" in str(e.value), f"Failed to raise exception: {cmd}"
        raise Exception(e)


def test_command_success():
    result = Command(["ls", "."]).run()
    assert isinstance(result, subprocess.CompletedProcess)


def test_command_success_custom():
    result = Command(["ls", "."], msg="Custom message").run()
    assert isinstance(result, subprocess.CompletedProcess)


def test_command_warn_default():
    result = Command(["ls", "non_existent_file"], msg="Custom message", warn=True).run()
    assert result.returncode != 0, result


def test_command_warn_customz():
    result = Command(["ls", "non_existent_file"], warn=True).run()
    assert result.returncode != 0, result


def test_command_repr():
    cmd = Command(["ls", "."])
    assert repr(cmd) == "Command(['ls', '.'])", repr(cmd)


def test_get_entry_point():
    tests = [
        ("Fake Project", "fake-project"),
        ("Fake Project 123", "fake-project"),
        ("Fake Project 123 456", "fake-project"),
        ("Fake Project #%", "fake-project"),
        ("# Fake Project #%", "fake-project"),
    ]
    for project_name, expected in tests:
        entry_point = get_entry_point(project_name)
        assert entry_point == expected, entry_point


def test_update_list():
    old = [
        {"task_key": "task1", "job_cluster_key": "cluster1"},
        {"task_key": "task2", "job_cluster_key": "cluster2"},
        {"task_key": "task3", "job_cluster_key": "cluster3"},
    ]
    new = [
        {"task_key": "task1", "job_cluster_key": "cluster4"},
    ]
    result = _update_list_by_key(old, new, "task_key")
    assert result == [
        {"task_key": "task1", "job_cluster_key": "cluster4"},
        {"task_key": "task2", "job_cluster_key": "cluster2"},
        {"task_key": "task3", "job_cluster_key": "cluster3"},
    ], result


def test_update_list_default():
    old = [
        {"task_key": "task1"},
        {"task_key": "task2"},
        {"task_key": "task3"},
    ]
    new = [
        {"task_key": "task1", "job_cluster_key": "cluster4"},
    ]
    result = _update_list_by_key(old, new, "task_key", {"job_cluster_key": "cluster1"})
    assert result == [
        {"task_key": "task1", "job_cluster_key": "cluster4"},
        {"task_key": "task2", "job_cluster_key": "cluster1"},
        {"task_key": "task3", "job_cluster_key": "cluster1"},
    ], result


def test_sort_dict():
    old = {
        "c": 1,
        "a": 2,
        "b": 3,
    }
    result = sort_dict(old, ["a", "b", "c"])

    assert result == {
        "a": 2,
        "b": 3,
        "c": 1,
    }, result


def test_require_databricks_run_script():
    assert not require_databricks_run_script(
        [0, 19, 8]
    ), f"Should NOT require run script - {KEDRO_VERSION}"


def test_require_databricks_run_script_fail():
    assert require_databricks_run_script(
        [0, 19, 6]
    ), f"Should require run script - {KEDRO_VERSION}"


def test_make_workflow_name():
    tests = [
        ("package", "__default__", "package"),
        ("package", "pipeline", "package_pipeline"),
    ]
    for package_name, pipeline_name, expected in tests:
        workflow_name = make_workflow_name(package_name, pipeline_name)
        assert workflow_name == expected, workflow_name


def test_remove_nulls_from_dict():
    a = {
        "a": 1,
        "b": None,
        "c": {"d": None},
        "e": {"f": {"g": None}},
        "h": {"i": {"j": {}}},
        "k": [],
        "l": [1, 2, 3],
        "m": [1, None, 3],
        "n": [1, {"o": None}, 3],
        "o": [1, [None], 3],
        "p": [1, {"q": {"r": None}}, 3],
    }

    assert remove_nulls(a) == {
        "a": 1,
        "l": [1, 2, 3],
        "m": [1, 3],
        "n": [1, 3],
        "o": [1, 3],
        "p": [1, 3],
    }, remove_nulls(a)


def test_is_null_or_empty():
    assert _is_null_or_empty(None), "Failed to check None"
    assert _is_null_or_empty({}), "Failed to check empty dict"
    assert _is_null_or_empty([]), "Failed to check empty list"
    assert not _is_null_or_empty(1), "Failed to check int"
    assert not _is_null_or_empty("a"), "Failed to check str"
    assert not _is_null_or_empty({1: 1}), "Failed to check dict"
    assert not _is_null_or_empty([1]), "Failed to check list"


@pytest.mark.parametrize(
    ["dct", "overrides", "expected"],
    [
        (
            {"a": 1, "b": 2},
            {"a": 3},
            {"a": 3, "b": 2},
        ),
        (
            {"a": 1, "b": 2},
            {"c": 3},
            {"a": 1, "b": 2, "c": 3},
        ),
        (
            {"a": 1, "b": 2},
            {"a": 3, "b": 4},
            {"a": 3, "b": 4},
        ),
        (
            {"a": 1, "b": 2},
            {"a": 3, "b": {"c": 4}},
            {"a": 3, "b": {"c": 4}},
        ),
        (
            {"a": 1, "b": 2},
            {"a": 3, "job_clusters": [{"job_cluster_key": "cluster1"}]},
            {"a": 3, "b": 2, "job_clusters": [{"job_cluster_key": "cluster1"}]},
        ),
        (
            {"a": 1, "b": {"c": 2}},
            {
                "a": 3,
                "b": {"c": 3},
                "job_clusters": [{"job_cluster_key": "cluster1"}],
            },
            {"a": 3, "b": {"c": 3}, "job_clusters": [{"job_cluster_key": "cluster1"}]},
        ),
    ],
)
def test_override_workflow(dct, overrides, expected):
    result = _override_workflow(dct, overrides, {}, "default")
    assert result == expected, result


def test_override_workflow_fail():
    with pytest.raises(TypeError):
        _override_workflow(None, {})  # type: ignore
    with pytest.raises(AttributeError):
        _override_workflow({}, None)  # type: ignore
    with pytest.raises(AttributeError):
        _override_workflow({}, None, None)  # type: ignore
