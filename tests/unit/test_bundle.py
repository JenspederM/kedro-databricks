from __future__ import annotations

import copy

from kedro.pipeline import Pipeline, node

from kedro_databricks.bundle import BundleController
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


def _generate_task(task_key: int | str, depends_on: list[str] = [], conf: str = "conf"):
    entry_point = "fake-project"
    params = [
        "--nodes",
        task_key,
        "--conf-source",
        f"/dbfs/FileStore/fake_project/{conf}",
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


def generate_workflow(conf="conf"):
    tasks = []

    for i in range(5):
        if i == 0:
            depends_on = []
        else:
            depends_on = ["node0"]
        tasks.append(_generate_task(f"node{i}", depends_on, conf))

    return {
        "name": "workflow1",
        "tasks": tasks,
        "format": "MULTI_TASK",
    }


WORKFLOW = generate_workflow()

OVERRIDES = {
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

WORKFLOW_OVERRIDES = {
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

MIX_OVERRIDES = {
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


def test_apply_resource_overrides(metadata):
    resources, result = _generate_testdata()
    controller = BundleController(metadata, "fake_env", "conf")
    controller.conf = OVERRIDES
    assert controller.apply_overrides(resources, "default") == {
        "workflow1": {"resources": {"jobs": {"workflow1": result}}}
    }, "Failed to apply default overrides"
    controller.conf = WORKFLOW_OVERRIDES
    assert controller.apply_overrides(resources, "default") == {
        "workflow1": {"resources": {"jobs": {"workflow1": result}}}
    }, "Failed to apply workflow overrides"
    controller.conf = MIX_OVERRIDES
    assert controller.apply_overrides(resources, "default") == {
        "workflow1": {"resources": {"jobs": {"workflow1": result}}}
    }, "Failed to apply mixed overrides"


def test_generate_workflow(metadata):
    controller = BundleController(metadata, "fake_env", "conf")
    assert controller._create_workflow("workflow1", pipeline) == WORKFLOW


def test_create_task(metadata):
    controller = BundleController(metadata, "fake_env", "conf")
    expected_task = _generate_task("task", ["a", "b"])
    assert controller._create_task("task", [
        node(identity, ["input"], ["output"], name="b"),
        node(identity, ["input"], ["output"], name="a")]) == expected_task


def test_generate_resources(metadata):
    controller = BundleController(metadata, "fake_env", "conf")
    controller.pipelines = {"__default__": Pipeline([])}
    assert controller.generate_resources(pipeline_name=None, MSG="Test MSG") == {}
    controller.pipelines = {
        "__default__": Pipeline([node(identity, ["input"], ["output"], name="node")])
    }
    assert controller.generate_resources(pipeline_name=None, MSG="Test MSG") == {
        "fake_project": {
            "resources": {
                "jobs": {
                    "fake_project": {
                        "format": "MULTI_TASK",
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
    controller = BundleController(metadata, "fake_env", "sub_conf")
    controller.pipelines = {
        "__default__": Pipeline([node(identity, ["input"], ["output"], name="node")])
    }

    assert controller.generate_resources(pipeline_name=None, MSG="Test MSG") == {
        "fake_project": {
            "resources": {
                "jobs": {
                    "fake_project": {
                        "format": "MULTI_TASK",
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
    controller = BundleController(metadata, "fake_env", "conf")
    controller.pipelines = {
        "__default__": Pipeline(
            [
                node(identity, ["input"], ["b_output"], name="b_node"),
                node(identity, ["input"], ["a_output"], name="a_node"),
            ]
        )
    }
    assert controller.generate_resources(pipeline_name=None, MSG="Test MSG") == {
        "fake_project": {
            "resources": {
                "jobs": {
                    "fake_project": {
                        "format": "MULTI_TASK",
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
    controller = BundleController(metadata, "fake_env", "conf")
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
    assert controller.generate_resources(
        pipeline_name="b_pipeline", MSG="Test MSG"
    ) == {
        "fake_project_b_pipeline": {
            "resources": {
                "jobs": {
                    "fake_project_b_pipeline": {
                        "format": "MULTI_TASK",
                        "name": "fake_project_b_pipeline",
                        "tasks": [
                            _generate_task("b_node"),
                        ],
                    },
                },
            },
        },
    }


def test_save_resoureces(metadata):
    controller = BundleController(metadata, "fake_env", "conf")

    resources = controller.generate_resources(None, "")
    bundle_resources = controller.apply_overrides(resources, "default")
    controller.save_bundled_resources(bundle_resources, True)
    resource_dir = metadata.project_path.joinpath("resources")
    assert resource_dir.exists(), "Failed to create resources directory"
    assert resource_dir.is_dir(), "resouces is not a directory"

    project_resources = resource_dir.joinpath(f"{metadata.package_name}.yml")
    project_files = ",".join([str(p) for p in resource_dir.iterdir()])
    assert (
        project_resources.exists()
    ), f"Failed to save project resources, {project_files}"
