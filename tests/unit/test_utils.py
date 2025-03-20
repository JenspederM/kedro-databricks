from __future__ import annotations

import logging
import os
import platform
import shutil
import subprocess

import pytest

from kedro_databricks.constants import OVERRIDE_KEY_MAP
from kedro_databricks.utils.bundle_helpers import (
    get_entry_point,
    remove_nulls,
    require_databricks_run_script,
    sort_dict,
)
from kedro_databricks.utils.common import Command, make_workflow_name
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


@pytest.mark.parametrize(
    ["key", "expected"],
    [
        ("unknown", None),
        (123, None),
        *[(key, value) for key, value in OVERRIDE_KEY_MAP.items()],
    ],
)
def test_get_lookup_key(key, expected):
    if expected is None:
        with pytest.raises(ValueError):
            _get_lookup_key(key)
        return
    result = _get_lookup_key(key)
    assert result == expected


@pytest.mark.parametrize(
    ["jobs", "overrides", "expected", "error"],
    [
        ({"workflow": "not_dict"}, {}, None, ValueError),
        ({"workflow": {}}, [], None, ValueError),
    ],
)
def test_override_resources(jobs, overrides, expected, error):
    resources = {"resources": {"jobs": jobs}}
    if error:
        with pytest.raises(error):
            override_resources(resources, overrides, "default")
    else:
        result = override_resources(resources, overrides, "default")
        assert result == expected


@pytest.mark.parametrize(
    ["actual", "expected"],
    [
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
    ],
)
def test_substitute_file_path(actual, expected):
    result = _substitute_file_path(actual)
    assert result == expected, f"\n{result}\n{expected}"


OS = platform.uname().system.lower()


@pytest.mark.parametrize(
    ["cmd", "result_code", "msg", "warn", "raises"],
    [
        (["ls", "."], 0, "", False, False),
        (["ls", "non_existent_file"], 2, "Custom message", False, True),
        (
            ["ls", "non_existent_file"],
            1 if OS == "darwin" else 2,
            "Custom message",
            True,
            False,
        ),
        (["ls", "non_existent_file"], 2, "", False, True),
        (["ls", "non_existent_file"], 1 if OS == "darwin" else 2, "", True, False),
    ],
)
def test_command(cmd, result_code, warn, msg, raises):
    if raises and not warn:
        with pytest.raises(Exception) as e:
            if msg:
                result = Command(cmd, msg=msg).run()
                assert msg in str(e.value)
            else:
                result = Command(cmd).run()
            assert isinstance(result, subprocess.CompletedProcess)
            assert result.returncode == result_code, result
    else:
        command = Command(cmd, warn=warn)
        assert repr(command) == f"Command({cmd})", repr(command)
        result = command.run()
        assert isinstance(result, subprocess.CompletedProcess)
        assert result.returncode == result_code, result


@pytest.mark.parametrize(
    ["actual", "expected"],
    [
        ("Fake Project", "fake-project"),
        ("Fake Project 123", "fake-project"),
        ("Fake Project 123 456", "fake-project"),
        ("Fake Project #%", "fake-project"),
        ("# Fake Project #%", "fake-project"),
    ],
)
def test_get_entry_point(actual, expected):
    entry_point = get_entry_point(actual)
    assert entry_point == expected, entry_point


@pytest.mark.parametrize(
    ["old", "new", "key", "default", "expected"],
    [
        ([], [], "task_key", {}, []),
        ([], [], "task_key", {"job_cluster_key": "cluster1"}, []),
        (
            [
                {"task_key": "task1", "job_cluster_key": "cluster1"},
                {"task_key": "task2", "job_cluster_key": "cluster2"},
                {"task_key": "task3", "job_cluster_key": "cluster3"},
            ],
            [
                {"task_key": "task1", "job_cluster_key": "cluster4"},
            ],
            "task_key",
            {},
            [
                {"task_key": "task1", "job_cluster_key": "cluster4"},
                {"task_key": "task2", "job_cluster_key": "cluster2"},
                {"task_key": "task3", "job_cluster_key": "cluster3"},
            ],
        ),
        (
            [
                {"task_key": "task1"},
                {"task_key": "task2"},
                {"task_key": "task3"},
            ],
            [
                {"task_key": "task1", "job_cluster_key": "cluster4"},
            ],
            "task_key",
            {"job_cluster_key": "cluster1"},
            [
                {"task_key": "task1", "job_cluster_key": "cluster4"},
                {"task_key": "task2", "job_cluster_key": "cluster1"},
                {"task_key": "task3", "job_cluster_key": "cluster1"},
            ],
        ),
    ],
)
def test_update_list(old, new, key, default, expected):
    result = _update_list_by_key(old, new, key, default)
    assert result == expected, result


@pytest.mark.parametrize(
    ["actual", "order", "expected"],
    [
        (
            {
                "c": 1,
                "a": 2,
                "b": 3,
            },
            ["a", "b", "c"],
            {
                "a": 2,
                "b": 3,
                "c": 1,
            },
        ),
        (
            {
                "a": 1,
                "b": 2,
                "c": 3,
            },
            ["c", "b", "a"],
            {
                "c": 3,
                "b": 2,
                "a": 1,
            },
        ),
    ],
)
def test_sort_dict(actual, order, expected):
    result = sort_dict(actual, order)
    assert result == expected, result


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        ([0, 19, 8], False),
        ([0, 19, 6], True),
    ],
)
def test_require_databricks_run_script(value, expected):
    assert require_databricks_run_script(value) == expected, value


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


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        ([1, 2, 3], [1, 2, 3]),
        ([1, None, 3], [1, 3]),
        ([1, {"a": None}, 3], [1, 3]),
        ([1, [None], 3], [1, 3]),
        ([1, {"a": {"b": None}}, 3], [1, 3]),
        ({}, {}),
        ({"a": 1, "b": None}, {"a": 1}),
        ({"a": 1, "b": {"c": None}}, {"a": 1}),
        ({"a": 1, "b": {"c": {"d": None}}}, {"a": 1}),
    ],
)
def test_remove_nulls_from_dict(value, expected):
    assert remove_nulls(value) == expected


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


@pytest.mark.parametrize(
    ["args", "error"],
    [
        ([None, {}], TypeError),
        ([{}, None], AttributeError),
        ([{}, None, None], AttributeError),
    ],
)
def test_override_workflow_fail(args, error):
    with pytest.raises(error):
        _override_workflow(*args)
