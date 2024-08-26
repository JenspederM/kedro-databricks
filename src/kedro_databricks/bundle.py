from __future__ import annotations

import copy
import logging
from typing import Any

import yaml
from kedro.framework.startup import ProjectMetadata
from kedro.pipeline import Pipeline, node

from kedro_databricks.utils import (
    TASK_KEY_ORDER,
    WORKFLOW_KEY_ORDER,
    _remove_nulls_from_dict,
    _sort_dict,
)

DEFAULT = "default"


def _create_task(
    name: str, depends_on: list[node], package: str, env: str
) -> dict[str, Any]:
    """Create a Databricks task for a given node.

    Args:
        name (str): name of the node
        depends_on (List[Node]): list of nodes that the task depends on
        package (str): name of the package
        env (str): name of the env to be used by the task

    Returns:
        Dict[str, Any]: a Databricks task
    """
    ## Follows the Databricks REST API schema. See "tasks" in the link below
    ## https://docs.databricks.com/api/workspace/jobs/create

    task = {
        "task_key": name.replace(".", "_"),
        "libraries": [{"whl": "../dist/*.whl"}],
        "depends_on": [{"task_key": dep.name.replace(".", "_")} for dep in depends_on],
        "python_wheel_task": {
            "package_name": package,
            "entry_point": "databricks_run",
            "parameters": [
                "--nodes",
                name,
                "--conf-source",
                f"/dbfs/FileStore/{package}/conf",
                "--package-name",
                package,
                "--env",
                env,
            ],
        },
    }

    return _sort_dict(task, TASK_KEY_ORDER)


def _create_workflow(
    name: str, pipeline: Pipeline, package: str, env: str
) -> dict[str, Any]:
    """Create a Databricks workflow for a given pipeline.

    Args:
        name (str): name of the pipeline
        pipeline (Pipeline): Kedro pipeline object
        package (str): name of the package
        env (str): name of the env to be used by the tasks of the workflow

    Returns:
        Dict[str, Any]: a Databricks workflow
    """
    ## Follows the Databricks REST API schema
    ## https://docs.databricks.com/api/workspace/jobs/create
    workflow = {
        "name": name,
        "tasks": [
            _create_task(node.name, depends_on=deps, package=package, env=env)
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

    return _remove_nulls_from_dict(_sort_dict(workflow, WORKFLOW_KEY_ORDER))


def _get_value_by_key(
    lst: list[dict[str, Any]], lookup: str, key: str
) -> dict[str, Any]:
    result = {}
    for d in lst:
        if d.get(lookup) == key:
            result = copy.deepcopy(d)
            break
    if result.get(lookup):
        result.pop(lookup)
    return result


def save_bundled_resources(
    resources: dict[str, dict[str, Any]],
    metadata: ProjectMetadata,
    overwrite: bool = False,
):
    """Save the generated resources to the project directory.

    Args:
        resources (Dict[str, Dict[str, Any]]): A dictionary of pipeline names and their Databricks resources
        metadata (ProjectMetadata): The metadata of the project
        overwrite (bool): Whether to overwrite existing resources
    """
    log = logging.getLogger(metadata.package_name)
    resources_dir = metadata.project_path / "resources"
    resources_dir.mkdir(exist_ok=True)
    for name, resource in resources.items():
        MSG = f"Writing resource '{name}'"
        p = resources_dir / f"{name}.yml"

        if p.exists() and not overwrite:  # pragma: no cover
            log.warning(
                f"{MSG}: {p.relative_to(metadata.project_path)} already exists."
                " Use --overwrite to replace."
            )
            continue

        with open(p, "w") as f:
            log.info(f"{MSG}: Wrote {p.relative_to(metadata.project_path)}")
            yaml.dump(resource, f, default_flow_style=False, indent=4, sort_keys=False)


def apply_resource_overrides(
    resources: dict[str, Any],
    overrides: dict[str, Any],
    default_key: str = DEFAULT,
):
    """Apply overrides to the Databricks resources.

    Args:
        resources (Dict[str, Any]): dictionary of Databricks resources
        overrides (Dict[str, Any]): dictionary of overrides
        default_key (str, optional): default key to use for overrides

    Returns:
        Dict[str, Any]: dictionary of Databricks resources with overrides applied
    """
    default_workflow = overrides.pop(default_key, {})
    default_tasks = default_workflow.get("tasks", [])
    default_task = _get_value_by_key(default_tasks, "task_key", default_key)

    for name, resource in resources.items():
        workflow = resource["resources"]["jobs"][name]
        workflow_overrides = copy.deepcopy(default_workflow)
        workflow_overrides.update(overrides.get(name, {}))
        workflow_task_overrides = workflow_overrides.pop("tasks", [])
        task_overrides = _get_value_by_key(
            workflow_task_overrides, "task_key", default_key
        )
        if not task_overrides:
            task_overrides = default_task

        resources[name]["resources"]["jobs"][name] = _apply_overrides(
            workflow, workflow_overrides, default_task=task_overrides
        )

    return resources


def generate_resources(
    pipelines: dict[str, Pipeline], metadata: ProjectMetadata, env: str, MSG: str
) -> dict[str, dict[str, Any]]:
    """Generate Databricks resources for the given pipelines.

    Finds all pipelines in the project and generates Databricks asset bundle resources
    for each according to the Databricks REST API

    Args:
        pipelines (dict[str, Pipeline]): A dictionary of pipeline names and their Kedro pipelines
        env (str): The name of the kedro environment to be used by the workflow

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
        wf = _create_workflow(name=wf_name, pipeline=pipeline, package=package, env=env)
        log.debug(f"Workflow '{wf_name}' successfully created.")
        log.debug(wf)
        workflows[wf_name] = wf

    resources = {
        name: {"resources": {"jobs": {name: wf}}} for name, wf in workflows.items()
    }

    log.info(f"{MSG}: Databricks resources successfully generated.")
    log.debug(resources)
    return resources
