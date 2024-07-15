import copy
import logging
from typing import Any

from kedro.framework.project import PACKAGE_NAME
from kedro.framework.startup import ProjectMetadata
from kedro.pipeline import Pipeline, node

from kedro_databricks.utils import (
    TASK_KEY_ORDER,
    WORKFLOW_KEY_ORDER,
    _remove_nulls_from_dict,
    _sort_dict,
)

DEFAULT = "default"


def _create_task(name: str, depends_on: list[node]) -> dict[str, Any]:
    """Create a Databricks task for a given node.

    Args:
        name (str): name of the node
        depends_on (List[Node]): list of nodes that the task depends on

    Returns:
        Dict[str, Any]: a Databricks task
    """
    ## Follows the Databricks REST API schema. See "tasks" in the link below
    ## https://docs.databricks.com/api/workspace/jobs/create

    task = {
        "task_key": name,
        "libraries": [{"whl": "../dist/*.whl"}],
        "depends_on": [{"task_key": dep.name} for dep in depends_on],
        "python_wheel_task": {
            "package_name": PACKAGE_NAME,
            "entry_point": "databricks_run",
            "parameters": [
                "--nodes",
                name,
                "--conf-source",
                f"/dbfs/FileStore/{PACKAGE_NAME}/conf",
                "--package-name",
                PACKAGE_NAME,
            ],
        },
    }

    return _sort_dict(task, TASK_KEY_ORDER)


def _create_workflow(name: str, pipeline: Pipeline) -> dict[str, Any]:
    """Create a Databricks workflow for a given pipeline.

    Args:
        name (str): name of the pipeline
        pipeline (Pipeline): Kedro pipeline object

    Returns:
        Dict[str, Any]: a Databricks workflow
    """
    ## Follows the Databricks REST API schema
    ## https://docs.databricks.com/api/workspace/jobs/create
    workflow = {
        "name": name,
        "tasks": [
            _create_task(node.name, depends_on=deps)
            for node, deps in pipeline.node_dependencies.items()
        ],
        "format": "MULTI_TASK",
    }

    return _remove_nulls_from_dict(_sort_dict(workflow, WORKFLOW_KEY_ORDER))


def _update_list(
    old: list[dict[str, Any]],
    new: list[dict[str, Any]],
    lookup_key: str,
    default: dict[str, Any] = {},
):
    assert isinstance(
        old, list
    ), f"old must be a list not {type(old)} for key: {lookup_key} - {old}"
    assert isinstance(
        new, list
    ), f"new must be a list not {type(new)} for key: {lookup_key} - {new}"
    from mergedeep import merge

    old_obj = {curr.pop(lookup_key): curr for curr in old}
    new_obj = {update.pop(lookup_key): update for update in new}
    keys = set(old_obj.keys()).union(set(new_obj.keys()))

    for key in keys:
        update = copy.deepcopy(default)
        update.update(new_obj.get(key, {}))
        new = merge(old_obj.get(key, {}), update)
        old_obj[key] = new

    return [{lookup_key: k, **v} for k, v in old_obj.items()]


def _apply_overrides(
    workflow: dict[str, Any],
    overrides: dict[str, Any],
    default_task: dict[str, Any] = {},
):
    from mergedeep import merge

    workflow["description"] = workflow.get("description", overrides.get("description"))
    workflow["edit_mode"] = workflow.get("edit_mode", overrides.get("edit_mode"))
    workflow["max_concurrent_runs"] = workflow.get(
        "max_concurrent_runs", overrides.get("max_concurrent_runs")
    )
    workflow["timeout_seconds"] = workflow.get(
        "timeout_seconds", overrides.get("timeout_seconds")
    )

    workflow["health"] = merge(workflow.get("health", {}), overrides.get("health", {}))
    workflow["email_notifications"] = merge(
        workflow.get("email_notifications", {}),
        overrides.get("email_notifications", {}),
    )
    workflow["webhook_notifications"] = merge(
        workflow.get("webhook_notifications", {}),
        overrides.get("webhook_notifications", {}),
    )
    workflow["notification_settings"] = merge(
        workflow.get("notification_settings", {}),
        overrides.get("notification_settings", {}),
    )
    workflow["schedule"] = merge(
        workflow.get("schedule", {}), overrides.get("schedule", {})
    )
    workflow["trigger"] = merge(
        workflow.get("trigger", {}), overrides.get("trigger", {})
    )
    workflow["continuous"] = merge(
        workflow.get("continuous", {}), overrides.get("continuous", {})
    )
    workflow["git_source"] = merge(
        workflow.get("git_source", {}), overrides.get("git_source", {})
    )
    workflow["tags"] = merge(workflow.get("tags", {}), overrides.get("tags", {}))
    workflow["queue"] = merge(workflow.get("queue", {}), overrides.get("queue", {}))
    workflow["run_as"] = merge(workflow.get("run_as", {}), overrides.get("run_as", {}))
    workflow["deployment"] = merge(
        workflow.get("deployment", {}), overrides.get("deployment", {})
    )

    workflow["access_control_list"] = merge(
        workflow.get("access_control_list", {}),
        overrides.get("access_control_list", {}),
    )

    workflow["tasks"] = _update_list(
        workflow.get("tasks", []), overrides.get("tasks", []), "task_key", default_task
    )
    workflow["job_clusters"] = _update_list(
        workflow.get("job_clusters", []),
        overrides.get("job_clusters", []),
        "job_cluster_key",
    )
    workflow["parameters"] = _update_list(
        workflow.get("parameters", []), overrides.get("parameters", []), "name"
    )
    workflow["environments"] = _update_list(
        workflow.get("environments", []),
        overrides.get("environments", []),
        "environment_key",
    )

    workflow["format"] = "MULTI_TASK"

    new_workflow = {}
    for k, v in workflow.items():
        if v is None or (isinstance(v, dict | list) and len(v) == 0):
            continue
        new_workflow[k] = v

    return new_workflow


def _get_value_by_key(lst: list[dict[str, Any]], lookup: str, key: str) -> Any:
    for d in lst:
        if d.get(lookup) == key:
            return d


def apply_resource_overrides(
    resources: dict[str, Any], overrides: dict[str, Any], default_key: str = DEFAULT
):
    default_workflow = overrides.pop(default_key, {})
    default_tasks = default_workflow.get("tasks", [])
    default_task = _get_value_by_key(default_tasks, "task_key", default_key)
    if default_task:
        del default_task["task_key"]
    else:
        default_task = {}

    for name, resource in resources.items():
        workflow = resource["resources"]["jobs"][name]
        workflow_overrides = copy.deepcopy(default_workflow)
        workflow_overrides.update(overrides.get(name, {}))
        task_overrides = workflow_overrides.pop("tasks", [])
        workflow_default_task = _get_value_by_key(
            task_overrides, "task_key", default_key
        )
        if workflow_default_task:
            del workflow_default_task["task_key"]
        else:
            workflow_default_task = copy.deepcopy(default_task)

        resources[name]["resources"]["jobs"][name] = _apply_overrides(
            workflow, workflow_overrides, default_task=workflow_default_task
        )

    return resources


def generate_resources(
    pipelines: dict[str, Pipeline], metadata: ProjectMetadata
) -> dict[str, dict[str, Any]]:
    """Generate Databricks resources for the given pipelines.

    Finds all pipelines in the project and generates Databricks asset bundle resources
    for each according to the Databricks REST API.

    Args:
        pipelines (dict[str, Pipeline]): A dictionary of pipeline names and their Kedro pipelines

    Returns:
        dict[str, dict[str, Any]]: A dictionary of pipeline names and their Databricks resources
    """
    log = logging.getLogger(metadata.package_name)

    package = metadata.package_name
    workflows = {}
    for name, pipeline in pipelines.items():
        if len(pipeline.nodes) == 0:
            continue

        wf_name = f"{package}_{name}" if name != "__default__" else package
        wf = _create_workflow(wf_name, pipeline)
        log.debug(f"Workflow '{wf_name}' successfully created.")
        log.debug(wf)
        workflows[wf_name] = wf

    resources = {
        name: {"resources": {"jobs": {name: wf}}} for name, wf in workflows.items()
    }

    log.info("Databricks resources successfully generated.")
    log.debug(resources)
    return resources
