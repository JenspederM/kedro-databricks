from __future__ import annotations

from kedro.pipeline import Pipeline, node

from kedro_databricks.bundle import BundleController
from kedro_databricks.utils.bundle_helpers import require_databricks_run_script


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


def _generate_task(
    task_key: int | str,
    depends_on: list[str] = [],
    conf: str = "conf",
    runtime_params: str = "",
):
    entry_point = "fake-project"
    params = [
        "--nodes",
        task_key,
        "--conf-source",
        "${workspace.file_path}/" + conf,  # type: ignore
        "--env",
        "fake_env",
    ]

    if require_databricks_run_script():
        entry_point = "databricks_run"
        params = params + ["--package-name", "fake_project"]

    if runtime_params:
        params = params + ["--params", runtime_params]

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
    }


WORKFLOW = generate_workflow()


def test_generate_workflow(metadata):
    controller = BundleController(metadata, "fake_env", "conf")
    assert controller._create_workflow("workflow1", pipeline) == WORKFLOW


def test_create_task(metadata):
    controller = BundleController(metadata, "fake_env", "conf")
    expected_task = _generate_task("task", ["a", "b"])
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


def test_create_task_with_runtime_params(metadata):
    controller = BundleController(
        metadata, "fake_env", "conf", runtime_params="key1=value1,key2=value2"
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
