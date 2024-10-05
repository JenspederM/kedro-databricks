from __future__ import annotations

import copy

from kedro.pipeline import Pipeline, node
from kedro_databricks.bundle import (
    _create_workflow,
    apply_resource_overrides,
    generate_resources,
)
from kedro_databricks.utils import require_databricks_run_script


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


def generate_task(task_key: int | str, depends_on: list[str] = []):
    entry_point = "fake-project"
    params = [
        "--nodes",
        task_key,
        "--conf-source",
        "/dbfs/FileStore/fake_project/conf",
        "--env",
        "fake_env",
    ]

    if require_databricks_run_script():
        entry_point = "databricks_run"
        params = params + ["--package-name", "fake_project"]

    task = {
        "task_key": task_key,
        "libraries": [
            {"whl": "../dist/*.whl"},
        ],
        "python_wheel_task": {
            "package_name": "fake_project",
            "entry_point": entry_point,
            "parameters": params,
        },
    }

    if len(depends_on) > 0:
        task["depends_on"] = [{"task_key": dep} for dep in depends_on]

    return task


def generate_workflow():
    tasks = []

    for i in range(5):
        if i == 0:
            depends_on = []
        else:
            depends_on = ["node0"]
        tasks.append(generate_task(f"node{i}", depends_on))

    return {
        "name": "workflow1",
        "tasks": tasks,
        "format": "MULTI_TASK",
    }


WORKFLOW = generate_workflow()

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
    WORKFLOW["name"]: {
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
    WORKFLOW["name"]: {
        "tasks": [
            {
                "task_key": "default",
                "job_cluster_key": "default",
            }
        ],
    },
}


def _generate_testdata():
    result = copy.deepcopy(WORKFLOW)
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
        WORKFLOW["name"]: {
            "resources": {"jobs": {WORKFLOW["name"]: copy.deepcopy(WORKFLOW)}}
        }
    }

    return resources, result


def test_apply_resource_overrides():
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


def test_generate_workflow(metadata):
    assert _create_workflow("workflow1", pipeline, metadata, "fake_env") == WORKFLOW


def test_generate_resources(metadata):
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
                            generate_task("node"),
                        ],
                    },
                },
            },
        },
    }
