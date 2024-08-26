from __future__ import annotations

import copy

import pytest
import yaml
from kedro.pipeline import Pipeline, node


def identity(arg):
    return arg


pipeline = Pipeline(
    [
        node(
            identity,
            ["input"],
            ["intermediate"],
            name="node0",
            tags=["tag0", "tag1"],
        ),
        node(identity, ["intermediate"], ["output"], name="node1"),
        node(identity, ["intermediate"], ["output2"], name="node2", tags=["tag0"]),
        node(
            identity,
            ["intermediate"],
            ["output3"],
            name="node3",
            tags=["tag1", "tag2"],
        ),
        node(identity, ["intermediate"], ["output4"], name="node4", tags=["tag2"]),
    ]
)

workflow = {
    "name": "workflow1",
    "tasks": [
        {
            "task_key": "node0",
            "libraries": [
                {"whl": "../dist/*.whl"},
            ],
            "python_wheel_task": {
                "package_name": "fake_project",
                "entry_point": "databricks_run",
                "parameters": [
                    "--nodes",
                    "node0",
                    "--conf-source",
                    "/dbfs/FileStore/fake_project/conf",
                    "--package-name",
                    "fake_project",
                    "--env",
                    "fake_env",
                ],
            },
        },
        {
            "task_key": "node1",
            "depends_on": [{"task_key": "node0"}],
            "libraries": [
                {"whl": "../dist/*.whl"},
            ],
            "python_wheel_task": {
                "package_name": "fake_project",
                "entry_point": "databricks_run",
                "parameters": [
                    "--nodes",
                    "node1",
                    "--conf-source",
                    "/dbfs/FileStore/fake_project/conf",
                    "--package-name",
                    "fake_project",
                    "--env",
                    "fake_env",
                ],
            },
        },
        {
            "task_key": "node2",
            "depends_on": [{"task_key": "node0"}],
            "libraries": [
                {"whl": "../dist/*.whl"},
            ],
            "python_wheel_task": {
                "package_name": "fake_project",
                "entry_point": "databricks_run",
                "parameters": [
                    "--nodes",
                    "node2",
                    "--conf-source",
                    "/dbfs/FileStore/fake_project/conf",
                    "--package-name",
                    "fake_project",
                    "--env",
                    "fake_env",
                ],
            },
        },
        {
            "task_key": "node3",
            "depends_on": [{"task_key": "node0"}],
            "libraries": [
                {"whl": "../dist/*.whl"},
            ],
            "python_wheel_task": {
                "package_name": "fake_project",
                "entry_point": "databricks_run",
                "parameters": [
                    "--nodes",
                    "node3",
                    "--conf-source",
                    "/dbfs/FileStore/fake_project/conf",
                    "--package-name",
                    "fake_project",
                    "--env",
                    "fake_env",
                ],
            },
        },
        {
            "task_key": "node4",
            "depends_on": [{"task_key": "node0"}],
            "libraries": [
                {"whl": "../dist/*.whl"},
            ],
            "python_wheel_task": {
                "package_name": "fake_project",
                "entry_point": "databricks_run",
                "parameters": [
                    "--nodes",
                    "node4",
                    "--conf-source",
                    "/dbfs/FileStore/fake_project/conf",
                    "--package-name",
                    "fake_project",
                    "--env",
                    "fake_env",
                ],
            },
        },
    ],
    "format": "MULTI_TASK",
}

overrides = {
    "default": {
        "job_clusters": [
            {
                "job_cluster_key": "default",
                "new_cluster": {
                    "spark_version": "7.3.x-scala2.12",
                    "node_type_id": "Standard_DS3_v2",
                    "num_workers": 2,
                    "spark_env_vars": {
                        "KEDRO_LOGGING_CONFIG": "dbfs:/path/to/logging.yml"
                    },
                },
            }
        ],
        "tasks": [
            {
                "task_key": "default",
                "job_cluster_key": "default",
            }
        ],
    }
}

workflow_overrides = {
    workflow["name"]: {
        "job_clusters": [
            {
                "job_cluster_key": "default",
                "new_cluster": {
                    "spark_version": "7.3.x-scala2.12",
                    "node_type_id": "Standard_DS3_v2",
                    "num_workers": 2,
                    "spark_env_vars": {
                        "KEDRO_LOGGING_CONFIG": "dbfs:/path/to/logging.yml"
                    },
                },
            }
        ],
        "tasks": [
            {
                "task_key": "default",
                "job_cluster_key": "default",
            }
        ],
    }
}

mix_overrides = {
    "default": {
        "job_clusters": [
            {
                "job_cluster_key": "default",
                "new_cluster": {
                    "spark_version": "7.3.x-scala2.12",
                    "node_type_id": "Standard_DS3_v2",
                    "num_workers": 2,
                    "spark_env_vars": {
                        "KEDRO_LOGGING_CONFIG": "dbfs:/path/to/logging.yml"
                    },
                },
            }
        ]
    },
    workflow["name"]: {
        "tasks": [
            {
                "task_key": "default",
                "job_cluster_key": "default",
            }
        ],
    },
}


def _generate_testdata():
    result = copy.deepcopy(workflow)
    result["job_clusters"] = [
        {
            "job_cluster_key": "default",
            "new_cluster": {
                "spark_version": "7.3.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 2,
                "spark_env_vars": {"KEDRO_LOGGING_CONFIG": "dbfs:/path/to/logging.yml"},
            },
        }
    ]
    for task in result["tasks"]:
        task["job_cluster_key"] = "default"

    resources = {
        workflow["name"]: {
            "resources": {"jobs": {workflow["name"]: copy.deepcopy(workflow)}}
        }
    }

    return resources, result


def test_apply_resource_overrides():
    from kedro_databricks.bundle import apply_resource_overrides

    resources, result = _generate_testdata()

    assert apply_resource_overrides(resources, overrides, "default") == {
        "workflow1": {"resources": {"jobs": {"workflow1": result}}}
    }, "Failed to apply default overrides"
    assert apply_resource_overrides(resources, workflow_overrides, "default") == {
        "workflow1": {"resources": {"jobs": {"workflow1": result}}}
    }, "Failed to apply workflow overrides"
    assert apply_resource_overrides(resources, mix_overrides, "default") == {
        "workflow1": {"resources": {"jobs": {"workflow1": result}}}
    }, "Failed to apply mixed overrides"


def test_generate_workflow():
    from kedro_databricks.bundle import _create_workflow

    assert (
        _create_workflow("workflow1", pipeline, "fake_project", "fake_env") == workflow
    )


def test_generate_resources(metadata):
    from kedro_databricks.bundle import generate_resources

    assert (
        generate_resources(
            {"__default__": Pipeline([])}, metadata, "fake_env", "Test MSG"
        )
        == {}
    )
    assert generate_resources(
        {"__default__": Pipeline([node(identity, ["input"], ["output"], name="node")])},
        metadata,
        "fake_env",
        "Test MSG",
    ) == {
        "fake_project": {
            "resources": {
                "jobs": {
                    "fake_project": {
                        "format": "MULTI_TASK",
                        "name": "fake_project",
                        "tasks": [
                            {
                                "libraries": [
                                    {
                                        "whl": "../dist/*.whl",
                                    },
                                ],
                                "python_wheel_task": {
                                    "package_name": "fake_project",
                                    "entry_point": "databricks_run",
                                    "parameters": [
                                        "--nodes",
                                        "node",
                                        "--conf-source",
                                        "/dbfs/FileStore/fake_project/conf",
                                        "--package-name",
                                        "fake_project",
                                        "--env",
                                        "fake_env",
                                    ],
                                },
                                "task_key": "node",
                            },
                        ],
                    },
                },
            },
        },
    }


def test_substitute_catalog_paths(metadata):
    from kedro_databricks.init import substitute_catalog_paths

    catalog_path = metadata.project_path / "conf" / "base" / "catalog.yml"

    # Add some datasets
    catalog = {
        "test": {
            "type": "pandas.CSVDataSet",
            "filepath": "/dbfs/FileStore/my-project/data/01_raw/test.csv",
        },
        "test2": {
            "type": "pandas.CSVDataSet",
            "filepath": "/dbfs/FileStore/my-project/data/01_raw/test2.csv",
        },
    }

    with open(catalog_path, "w") as f:
        f.write(yaml.dump(catalog))

    substitute_catalog_paths(metadata)

    with open(catalog_path) as f:
        new_catalog = f.read()

    assert "/dbfs/FileStore/fake_project/data" in new_catalog, new_catalog


def test_run_cmd():
    import subprocess

    from kedro_databricks.utils import run_cmd

    with pytest.raises(Exception) as e:
        cmd = ["ls", "non_existent_file"]
        run_cmd(cmd)
        assert "Failed to run command" in str(
            e.value
        ), f"Failed to raise exception: {cmd}"
        raise e

    with pytest.raises(Exception) as e:
        cmd = ["ls", "non_existent_file"]
        run_cmd(cmd, msg="Custom message")
        assert "Custom message" in str(e.value), f"Failed to raise exception: {cmd}"
        raise e

    run_cmd(["ls", "non_existent_file"], warn=True)
    run_cmd(["ls", "non_existent_file"], msg="Custom message", warn=True)

    result = run_cmd(["ls", "."])
    assert isinstance(result, subprocess.CompletedProcess)

    result = run_cmd(["ls", "non_existent_file"], msg="Custom message", warn=True)
    assert result is None, result


def test_remove_nulls_from_dict():
    from kedro_databricks.utils import remove_nulls

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
    from kedro_databricks.utils import _is_null_or_empty

    assert _is_null_or_empty(None), "Failed to check None"
    assert _is_null_or_empty({}), "Failed to check empty dict"
    assert _is_null_or_empty([]), "Failed to check empty list"
    assert not _is_null_or_empty(1), "Failed to check int"
    assert not _is_null_or_empty("a"), "Failed to check str"
    assert not _is_null_or_empty({1: 1}), "Failed to check dict"
    assert not _is_null_or_empty([1]), "Failed to check list"
