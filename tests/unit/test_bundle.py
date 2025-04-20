from __future__ import annotations

import pytest
from kedro.pipeline import Pipeline, node

from kedro_databricks.cli.bundle import (
    _load_kedro_env_config,
    _save_bundled_resources,
    bundle,
)
from kedro_databricks.cli.bundle.generate_resources import (
    ResourceGenerator,
    remove_nulls,
    sort_dict,
)
from kedro_databricks.cli.bundle.override_resources import (
    _get_lookup_key,
    _override_workflow,
    _update_list_by_key,
    override_resources,
)
from kedro_databricks.cli.bundle.utils import get_entry_point
from kedro_databricks.constants import OVERRIDE_KEY_MAP
from kedro_databricks.utils import require_databricks_run_script
from tests.utils import WORKFLOW, _generate_task, identity, pipeline


def test_bundle(metadata):
    bundle(metadata=metadata, env="fake_env", default_key="default")


def test_generate_workflow(metadata):
    g = ResourceGenerator(metadata, "fake_env")
    assert g._create_workflow("workflow1", pipeline) == WORKFLOW


def test_create_task(metadata):
    g = ResourceGenerator(metadata, "fake_env")
    expected_task = _generate_task("task", ["a", "b"])
    node_a = node(identity, ["input"], ["output"], name="a")
    node_b = node(identity, ["input"], ["output"], name="b")
    assert (
        g._create_task(
            "task",
            [
                node_b,
                node_a,
            ],
        )
        == expected_task
    )


def test_create_task_with_runtime_params(metadata):
    controller = ResourceGenerator(
        metadata, "fake_env", params="key1=value1,key2=value2"
    )
    expected_task = _generate_task(
        "task", ["a", "b"], runtime_params="key1=value1,key2=value2"
    )
    node_a = node(identity, ["input"], ["output"], name="a")
    node_b = node(identity, ["input"], ["output"], name="b")
    assert (
        controller._create_task(
            "task",
            [
                node_b,
                node_a,
            ],
        )
        == expected_task
    )


def test_generate_resources(metadata):
    controller = ResourceGenerator(metadata, "fake_env")
    controller.pipelines = {"__default__": Pipeline([])}
    assert controller.generate_resources(pipeline_name=None) == {}
    controller.pipelines = {
        "__default__": Pipeline([node(identity, ["input"], ["output"], name="node")])
    }
    assert controller.generate_resources(pipeline_name=None) == {
        "fake_project": {
            "resources": {
                "jobs": {
                    "fake_project": {
                        "name": "fake_project",
                        "tasks": [
                            _generate_task("node"),
                        ],
                    },
                },
            },
        },
    }


def test_generate_resources_another_conf(metadata):
    controller = ResourceGenerator(metadata, "fake_env", "sub_conf")
    controller.pipelines = {
        "__default__": Pipeline([node(identity, ["input"], ["output"], name="node")])
    }

    assert controller.generate_resources(pipeline_name=None) == {
        "fake_project": {
            "resources": {
                "jobs": {
                    "fake_project": {
                        "name": "fake_project",
                        "tasks": [
                            _generate_task("node", conf="sub_conf"),
                        ],
                    },
                },
            },
        },
    }


def test_generate_resources_in_a_sorted_manner(metadata):
    controller = ResourceGenerator(metadata, "fake_env")
    controller.pipelines = {
        "__default__": Pipeline(
            [
                node(identity, ["input"], ["b_output"], name="b_node"),
                node(identity, ["input"], ["a_output"], name="a_node"),
            ]
        )
    }
    assert controller.generate_resources(pipeline_name=None) == {
        "fake_project": {
            "resources": {
                "jobs": {
                    "fake_project": {
                        "name": "fake_project",
                        "tasks": [
                            _generate_task("a_node"),
                            _generate_task("b_node"),
                        ],
                    },
                },
            },
        },
    }


def test_generate_resources_for_a_single_pipeline(metadata):
    controller = ResourceGenerator(metadata, "fake_env")
    controller.pipelines = {
        "__default__": Pipeline(
            [
                node(identity, ["input"], ["a_output"], name="a_node"),
            ]
        ),
        "a_pipeline": Pipeline(
            [
                node(identity, ["input"], ["a_output"], name="a_node"),
            ]
        ),
        "b_pipeline": Pipeline(
            [
                node(identity, ["input"], ["b_output"], name="b_node"),
            ]
        ),
    }
    assert controller.generate_resources(pipeline_name="b_pipeline") == {
        "fake_project_b_pipeline": {
            "resources": {
                "jobs": {
                    "fake_project_b_pipeline": {
                        "name": "fake_project_b_pipeline",
                        "tasks": [
                            _generate_task("b_node"),
                        ],
                    },
                },
            },
        },
    }


def test_save_resources(metadata):
    controller = ResourceGenerator(metadata, "fake_env")
    resources = controller.generate_resources()
    overrides = _load_kedro_env_config(metadata, "conf", "fake_env")
    result = {}
    for name, resource in resources.items():
        result[name] = override_resources(resource, overrides, "default")
    _save_bundled_resources(metadata, result, True)
    resource_dir = metadata.project_path.joinpath("resources")
    assert resource_dir.exists(), "Failed to create resources directory"
    assert resource_dir.is_dir(), "resouces is not a directory"

    project_resources = resource_dir.joinpath(f"{metadata.package_name}.yml")
    project_files = ",".join([str(p) for p in resource_dir.iterdir()])
    assert (
        project_resources.exists()
    ), f"Failed to save project resources, {project_files}"


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
