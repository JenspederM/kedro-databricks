import copy
from typing import Any

from kedro_databricks.constants import OVERRIDE_KEY_MAP


def override_resources(bundle: dict, overrides: dict, default_key):
    """Override the resources in a Databricks bundle.

    Args:
        bundle (Dict): the Databricks bundle
        overrides (Dict): the overrides to apply

    Returns:
        Dict: the Databricks bundle with the overrides applied
    """
    result = {"resources": {"jobs": {}}}
    for name, workflow in bundle.get("resources", {}).get("jobs", {}).items():
        if not isinstance(workflow, dict):
            raise ValueError(f"workflow must be a dictionary not {type(workflow)}")
        if not isinstance(overrides, dict):
            raise ValueError(f"overrides must be a dictionary not {type(overrides)}")
        workflow_overrides, task_overrides = _get_workflow_overrides(
            overrides, workflow, default_key
        )
        result["resources"]["jobs"][name] = _override_workflow(
            workflow=workflow,
            workflow_overrides=workflow_overrides,
            task_overrides=task_overrides,
            default_key=default_key,
        )
    return result


def _get_lookup_key(key: str):
    lookup = OVERRIDE_KEY_MAP.get(key)
    if lookup is None:
        raise ValueError(f"Key {key} not found in OVERRIDE_KEY_MAP")
    return lookup


def _get_defaults(lst: list, lookup_key: str, default_key):
    """Get the default dictionary from a list of dictionaries.

    Args:
        lst (List[Dict]): list of dictionaries
        lookup_key (str): key to use for looking up dictionaries

    Returns:
        Dict: the default dictionary
    """
    for d in lst:
        if isinstance(d, dict) and d.get(lookup_key, "") == default_key:
            return d
    return {}


def _update_list_by_key(
    old: list[dict[str, Any]],
    new: list[dict[str, Any]],
    lookup_key: str,
    default: dict[str, Any] = {},
    default_key: str = "default",
):
    """Update a list of dictionaries with another list of dictionaries.

    Args:
        old (List[Dict[str, Any]]): list of dictionaries to update
        new (List[Dict[str, Any]]): list of dictionaries to update with
        lookup_key (str): key to use for looking up dictionaries
        default (Dict[str, Any], optional): default dictionary to use for updating

    Returns:
        List[Dict[str, Any]]: updated list of dictionaries
    """
    assert isinstance(
        old, list
    ), f"old must be a list not {type(old)} for key: {lookup_key} - {old}"
    assert isinstance(
        new, list
    ), f"new must be a list not {type(new)} for key: {lookup_key} - {new}"
    assert all(
        lookup_key in o for o in old
    ), f"lookup_key {lookup_key} not found in current: {old}"
    assert all(
        lookup_key in n for n in new
    ), f"lookup_key {lookup_key} not found in updates: {new}"

    old_obj = {curr.pop(lookup_key): curr for curr in copy.deepcopy(old)}
    new_obj = {update.pop(lookup_key): update for update in copy.deepcopy(new)}
    keys = set(old_obj.keys()).union(set(new_obj.keys()))

    for key in keys:
        if default and key == default_key:
            continue
        update = copy.deepcopy(default)
        update.pop(lookup_key, None)
        update.update(new_obj.get(key, {}))
        new = _override_workflow(old_obj.get(key, {}), update, {}, default_key)  # type: ignore
        old_obj[key] = new  # type: ignore

    return sorted(
        [{lookup_key: k, **v} for k, v in old_obj.items()], key=lambda x: x[lookup_key]
    )


def _get_old_value(result: Any, key: Any, value: Any):
    default = None
    if isinstance(value, dict):
        default = {}
    elif isinstance(value, list):
        default = []
    return result.get(key, default)


def _get_workflow_overrides(overrides, workflow, default_key):
    default_overrides = overrides.copy().pop(default_key, {})
    workflow_overrides = overrides.copy().pop(workflow.get("name"), {})
    all_overrides = {**default_overrides, **workflow_overrides}
    default_task = _get_defaults(
        all_overrides.get("tasks", []), "task_key", default_key
    )
    return all_overrides, default_task


def _override_workflow(
    workflow: dict,
    workflow_overrides: dict,
    task_overrides: dict = {},
    default_key: str = "default",
):
    """Override a Databricks workflow with the given overrides.

    Args:
        workflow (Dict): the Databricks workflow
        overrides (Dict): the overrides to apply

    Returns:
        Dict: the Databricks workflow with the overrides applied
    """
    result = {**workflow}

    for key, value in workflow_overrides.items():
        old_value = _get_old_value(result, key, value)
        if isinstance(value, dict) and isinstance(old_value, dict):
            result[key] = _override_workflow(
                workflow=old_value,
                workflow_overrides=value,
                task_overrides={},
                default_key=default_key,
            )
        elif isinstance(value, list) and isinstance(old_value, list):
            lookup_key = _get_lookup_key(key)
            if lookup_key:
                result[key] = _update_list_by_key(
                    old=old_value,
                    new=value,
                    lookup_key=_get_lookup_key(key),
                    default=task_overrides if key == "tasks" else {},
                    default_key=default_key,
                )
            else:  # pragma: no cover
                result[key] = value
        else:
            result[key] = workflow_overrides.get(key, old_value)
    return result
