import copy
from typing import Any

from kedro_databricks.core.constants import IGNORED_OVERRIDE_KEYS, OVERRIDE_KEY_MAP
from kedro_databricks.core.logger import get_logger

log = get_logger("bundle").getChild(__name__)


def override_job(job: dict, overrides: dict, default_key) -> dict[str, Any]:
    """Override the resources in a Databricks bundle.

    This function applies the given overrides to the resources in a Databricks bundle.

    Args:
        jobs (Dict): the Databricks jobs to override
        overrides (Dict): the overrides to apply
        default_key (str): the default key to use for overrides

    Raises:
        ValueError: if the job or overrides are not dictionaries
        ValueError: if the key in overrides is not found in OVERRIDE_KEY_MAP

    Returns:
        Dict[str, Any]: the Databricks bundle with the overrides applied
    """
    if not isinstance(job, dict):
        raise ValueError(f"job must be a dictionary not {type(job)}")
    if not isinstance(overrides, dict):
        raise ValueError(f"overrides must be a dictionary not {type(overrides)}")
    job_overrides, task_overrides = _get_job_overrides(overrides, job, default_key)
    return _override_job(
        job=job,
        job_overrides=job_overrides,
        task_overrides=task_overrides,
        default_key=default_key,
    )


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
    _validate_list_by_key(old=old, new=new, lookup_key=lookup_key)
    old_obj = {curr.pop(lookup_key): curr for curr in copy.deepcopy(old)}
    new_obj = {update.pop(lookup_key): update for update in copy.deepcopy(new)}
    keys = set(old_obj.keys()).union(set(new_obj.keys()))

    for key in keys:
        if default and key == default_key:
            continue
        update = copy.deepcopy(default)
        update.pop(lookup_key, None)
        update.update(new_obj.get(key, {}))
        new = _override_job(old_obj.get(key, {}), update, {}, default_key)  # type: ignore
        old_obj[key] = new  # type: ignore

    return sorted(
        [{lookup_key: k, **v} for k, v in old_obj.items()], key=lambda x: x[lookup_key]
    )


def _validate_list_by_key(
    old: list[dict[str, Any]], new: list[dict[str, Any]], lookup_key: str
):
    """Validate that a list of dictionaries contains the lookup key.

    Args:
        old (List[Dict[str, Any]]): list of dictionaries to validate
        new (List[Dict[str, Any]]): list of dictionaries to validate
        lookup_key (str): key to use for looking up dictionaries

    Raises:
        ValueError: if the lookup key is not found in any dictionary
    """
    assert isinstance(old, list), (
        f"old must be a list not {type(old)} for key: {lookup_key} - {old}"
    )
    assert isinstance(new, list), (
        f"new must be a list not {type(new)} for key: {lookup_key} - {new}"
    )
    assert all(lookup_key in o for o in old), (
        f"lookup_key {lookup_key} not found in current: {old}"
    )
    assert all(lookup_key in n for n in new), (
        f"lookup_key {lookup_key} not found in updates: {new}"
    )


def _get_old_value(result: Any, key: Any, value: Any):
    default = None
    if isinstance(value, dict):
        default = {}
    elif isinstance(value, list):
        default = []
    return result.get(key, default)


def _get_job_overrides(overrides, job, default_key):
    default_overrides = overrides.copy().pop(default_key, {})
    job_overrides = overrides.copy().pop(job.get("name"), {})
    all_overrides = {**default_overrides, **job_overrides}
    default_task = _get_defaults(
        all_overrides.get("tasks", []), "task_key", default_key
    )
    return all_overrides, default_task


def _override_job(
    job: dict,
    job_overrides: dict,
    task_overrides: dict = {},
    default_key: str = "default",
):
    """Override a Databricks job with the given overrides.

    Args:
        job (Dict): the Databricks job
        overrides (Dict): the overrides to apply

    Returns:
        Dict: the Databricks job with the overrides applied
    """
    result = {**job}

    for key, value in job_overrides.items():
        old_value = _get_old_value(result, key, value)
        if isinstance(value, dict) and isinstance(old_value, dict):
            result[key] = _override_job(
                job=old_value,
                job_overrides=value,
                task_overrides={},
                default_key=default_key,
            )
        elif isinstance(value, list) and isinstance(old_value, list):
            if isinstance(value[0], dict) and key not in IGNORED_OVERRIDE_KEYS:
                lookup_key = _get_lookup_key(key)
                if lookup_key:
                    result[key] = _update_list_by_key(
                        old=old_value,
                        new=value,
                        lookup_key=lookup_key,
                        default=task_overrides if key == "tasks" else {},
                        default_key=default_key,
                    )
            elif key == "parameters":
                # Special case for parameters, which can be a list of strings
                log.debug(f"Overriding parameters for key {key} with value {value}")
                result[key] = old_value + value
            else:
                result[key] = value
        else:
            result[key] = job_overrides.get(key, old_value)
    return result
