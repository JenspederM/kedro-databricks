from __future__ import annotations

import shutil

from kedro.framework.startup import ProjectMetadata
from kedro.pipeline import Pipeline, node

from kedro_databricks.utils import require_databricks_run_script


def reset_init(metadata: ProjectMetadata):
    (metadata.project_path / "databricks.yml").unlink(missing_ok=True)
    shutil.rmtree(metadata.project_path / "conf" / "dev", ignore_errors=True)
    shutil.rmtree(metadata.project_path / "conf" / "prod", ignore_errors=True)


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
        "/Workspace/${workspace.file_path}/" + conf,
        "--env",
        "${var.environment}",
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
