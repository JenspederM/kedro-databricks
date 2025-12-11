import pytest
from packaging.version import Version

from kedro_databricks.constants import OVERRIDE_KEY_MAP
from kedro_databricks.utilities.common import (
    get_arg_value,
    get_entry_point,
    get_lookup_key,
    get_value_from_dotpath,
    remove_nulls,
    sanitize_name,
    sort_dict,
    update_list_by_key,
    version_to_str,
)
from tests.utils import identity, node, require_databricks_run_script


@pytest.mark.parametrize(
    "node, expected",
    [
        (node(identity, ["input"], ["output"], name="a"), "a"),
        (node(identity, ["input"], ["output"], name="a", namespace="abc"), "abc_a"),
        (node(identity, ["input"], ["output"], name="a" * 150), "a" * 100),
        ("a" * 150, "a" * 100),
    ],
)
def test_sanitize_name(node, expected):
    sanname = sanitize_name(node)
    assert sanname == expected, f"Expected '{expected}', got '{sanname}'"


@pytest.mark.parametrize(
    ["actual", "expected"],
    [
        ("Fake Project", "fake-project"),
        ("Fake Project 123", "fake-project"),
        ("Fake Project 123 456", "fake-project"),
        ("Fake Project #%", "fake-project"),
        ("# Fake Project #%", "fake-project"),
        ("my-project-package", "my-project-package"),
        ("my_project_package", "my_project_package"),
    ],
)
def test_get_entry_point(actual, expected):
    entry_point = get_entry_point(actual)
    assert entry_point == expected, entry_point


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
        (Version("0.19.8"), False),
        (Version("0.19.6"), True),
    ],
)
def test_require_databricks_run_script(value, expected):
    assert require_databricks_run_script(value) == expected, value


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
            version_to_str(version)
    else:
        result = version_to_str(version)
        assert result == expected, f"Expected {expected}, but got {result}"


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
    result = get_arg_value(args, arg)
    assert result == expected, f"Expected {expected}, but got {result}"


@pytest.mark.parametrize(
    ["key", "lookup_map", "expected"],
    [
        ("unknown", {}, None),
        (123, {}, None),
        *[(key, OVERRIDE_KEY_MAP, value) for key, value in OVERRIDE_KEY_MAP.items()],
    ],
)
def test_get_lookup_key(key, lookup_map, expected):
    if expected is None:
        with pytest.raises(ValueError):
            get_lookup_key(key, lookup_map)
        return
    result = get_lookup_key(key, lookup_map)
    assert result == expected


@pytest.mark.parametrize(
    ["old", "new", "key", "default", "expected"],
    [
        (
            [],
            [],
            "task_key",
            {},
            [],
        ),
        (
            [],
            [],
            "task_key",
            {"job_cluster_key": "cluster1"},
            [],
        ),
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
    def _cb(old, new, default, key):
        return {**old, **new, **default}

    result = update_list_by_key(
        old=old,
        new=new,
        lookup_key=key,
        default=default,
        callback=_cb,
    )
    assert result == expected, result


@pytest.mark.parametrize(
    ["dotpath", "conf", "expected"],
    [
        ("a.b.c", {"a": {"b": {"c": 1}}}, 1),
        ("a.b.c", {"a": {"b": {"c": {"d": 2}}}}, {"d": 2}),
        ("a.b.c.d", {"a": {"b": {"c": {"d": 2}}}}, 2),
        ("a.b.x", {"a": {"b": {"c": 1}}}, None),
        ("a.b.c", {}, None),
        ("a.b.c", [], None),
        ("a.b.c", None, None),
    ],
)
def test_get_value_from_dotpath(dotpath, conf, expected):
    value = get_value_from_dotpath(conf, dotpath)
    assert value == expected, f"Expected {expected}, got {value}"
