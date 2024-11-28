from __future__ import annotations

import subprocess

import pytest

from kedro_databricks.utils import (
    KEDRO_VERSION,
    Command,
    _is_null_or_empty,
    _sort_dict,
    get_entry_point,
    make_workflow_name,
    remove_nulls,
    require_databricks_run_script,
    update_list,
)


def test_command_fail_default():
    with pytest.raises(Exception) as e:
        cmd = ["ls", "non_existent_file"]
        Command(cmd).run()
        assert "Failed to run command" in str(
            e.value
        ), f"Failed to raise exception: {cmd}"
        raise e


def test_command_fail_custom():
    with pytest.raises(Exception) as e:
        cmd = ["ls", "non_existent_file"]
        msg = "Custom message"
        Command(cmd, msg=msg).run()
        assert "Custom message" in str(e.value), f"Failed to raise exception: {cmd}"
        raise e


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
    result = update_list(old, new, "task_key")
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
    result = update_list(old, new, "task_key", {"job_cluster_key": "cluster1"})
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
    result = _sort_dict(old, ["a", "b", "c"])

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
