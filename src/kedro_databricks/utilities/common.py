from __future__ import annotations

import copy
import re
from collections.abc import Callable
from typing import Any

from kedro.pipeline.node import Node
from packaging.version import Version

from kedro_databricks.constants import KEDRO_VERSION, MAX_TASK_KEY_LENGTH
from kedro_databricks.utilities.logger import get_logger

log = get_logger("utilities.common")


def require_databricks_run_script(_version=KEDRO_VERSION) -> bool:
    """Check if the current Kedro version is less than 0.19.8.

    Kedro 0.19.8 introduced a new `run_script` method that is required for
    running tasks on Databricks. This method is not available in earlier
    versions of Kedro. This function checks if the current Kedro version is
    less than 0.19.8.

    Returns:
        bool: whether the current Kedro version is less than 0.19.8
    """
    return _version < Version("0.19.8")


def version_to_str(version: list[int]) -> str:
    if len(version) != 3:  # noqa: PLR2004 - Semantic versioning requires 3 parts
        raise ValueError(f"Invalid version: {version}")
    return ".".join(str(x) for x in version)


def get_arg_value(args: list[str], arg_name: str) -> str | None:
    for i, arg in enumerate(args):
        if "=" in arg:
            _arg, value = arg.split("=", 1)
            if _arg == arg_name:
                return value
        elif arg == arg_name:
            return args[i + 1]


def remove_nulls(value: dict[str, Any] | list[Any]) -> dict[str, Any] | list[Any]:
    """Remove None values from a dictionary or list.

    Args:
        value (Dict[Any, Any] | List[Dict[Any, Any]]): dictionary or list to remove None values from

    Returns:
        Dict[Any, Any] | List[Dict[Any, Any]]: dictionary or list with None values removed
    """
    non_null = copy.deepcopy(value)
    if isinstance(non_null, dict):
        _remove_nulls_from_dict(non_null)
    elif isinstance(non_null, list):
        _remove_nulls_from_list(non_null)
    return non_null


def _remove_nulls_from_list(lst: list) -> list:
    """Remove None values from a list.

    Args:
        l (List[Dict[Any, Any]]): list to remove None values from
    """
    for i, item in enumerate(lst):
        value = remove_nulls(item)
        if not value:
            del lst[i]
        else:
            lst[i] = value
    return lst


def _remove_nulls_from_dict(d: dict) -> dict:
    """Remove None values from a dictionary.

    Args:
        d (Dict[str, Any]): dictionary to remove None values from

    Returns:
        Dict[str, float | int | str | bool]: dictionary with None values removed
    """
    for k, v in list(d.items()):
        value = remove_nulls(v)
        if not value:
            del d[k]
        else:
            d[k] = value
    return d


def get_entry_point(project_name: str) -> str:
    """Get the entry point for a project.

    Args:
        project_name (str): name of the project

    Returns:
        str: entry point for the project
    """
    entrypoint = project_name.strip().lower()
    entrypoint = re.sub(r" +", " ", entrypoint)
    entrypoint = re.sub(r"[^a-zA-Z_]", "-", entrypoint)
    entrypoint = re.sub(r"(-+)$", "", entrypoint)
    entrypoint = re.sub(r"^(-+)", "", entrypoint)
    return entrypoint


def sort_dict(d: dict[Any, Any], key_order: list[str] | None = None) -> dict[Any, Any]:
    """Recursively sort the keys of a dictionary.

    Args:
        d (Dict[Any, Any]): dictionary to sort
        key_order (List[str]): list of keys to sort by

    Returns:
        Dict[Any, Any]: dictionary with ordered values
    """
    if key_order is None:
        key_order = []
    other_keys = [k for k in d.keys() if k not in key_order]
    order = key_order + other_keys

    return dict(sorted(d.items(), key=lambda x: order.index(x[0])))


def sanitize_name(node: Node | str) -> str:
    """Sanitize the node name to be used as a task key in Databricks.

    Args:
        node (Node | str): Kedro node object or node name

    Returns:
        str: sanitized task key
    """
    if isinstance(node, str):
        _name = node
    else:
        _name = node.name

    if not re.match(r"^[\w\-\_]+$", _name):  # Ensure the name is valid
        log.warning(
            f"Node name '{_name}' contains invalid characters and will be sanitized. "
            "To avoid this use an explicit node name as `node(..., name='valid_name')`."
        )

        _name = re.sub(r"[^\w\_]", "_", _name)
        _name = re.sub(r"_{2,}", "_", _name).lstrip("_").rstrip("_")

    if len(_name) > MAX_TASK_KEY_LENGTH:  # Ensure the name is not too long
        log.warning(
            f"Node name '{_name}' is too long. "
            f"Truncating to {MAX_TASK_KEY_LENGTH} characters."
        )
        _name = _name[:MAX_TASK_KEY_LENGTH]

    return _name


def get_lookup_key(key: str, lookup_map: dict):
    """Get the lookup key for a given key.

    Args:
        key (str): key to get the lookup key for
        lookup_map (Dict): map of keys to lookup keys

    Returns:
        str: lookup key for the given key
    """
    lookup = lookup_map.get(key)
    if lookup is None:
        raise ValueError(f"Key {key} not found in OVERRIDE_KEY_MAP")
    return lookup


def get_defaults(lst: list, lookup_key: str, default_key):
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


def update_list_by_key(
    old: list[dict[str, Any]],
    new: list[dict[str, Any]],
    lookup_key: str,
    callback: Callable[[Any, Any, dict[str, Any], str], Any],
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
    validate_list_by_key(old=old, new=new, lookup_key=lookup_key)
    old_obj = {curr.pop(lookup_key): curr for curr in copy.deepcopy(old)}
    new_obj = {update.pop(lookup_key): update for update in copy.deepcopy(new)}
    keys = set(old_obj.keys()).union(set(new_obj.keys()))

    for key in keys:
        if default and key == default_key:
            continue
        update = copy.deepcopy(default)
        update.pop(lookup_key, None)
        update.update(new_obj.get(key, {}))
        new = callback(old_obj.get(key, {}), update, {}, default_key)  # type: ignore
        old_obj[key] = new  # type: ignore

    return sorted(
        [{lookup_key: k, **v} for k, v in old_obj.items()],
        key=lambda x: x[lookup_key],
    )


def validate_list_by_key(
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


def get_old_value(result: Any, key: Any, value: Any):
    """Get the old value from a dictionary with a default based on the type of value.

    Args:
        result (Any): dictionary to get the old value from
        key (Any): key to get the old value for
        value (Any): value to determine the default type

    Returns:
        Any: old value from the dictionary or default value
    """
    default = None
    if isinstance(value, dict):
        default = {}
    elif isinstance(value, list):
        default = []
    return result.get(key, default)


def get_value_from_dotpath(validated_conf, dotpath):
    if not isinstance(validated_conf, dict):
        return None
    keys = dotpath.split(".")
    key = keys.pop(0)
    current_level = validated_conf.get(key)
    if current_level is None:
        return None
    elif len(keys) > 0:
        return get_value_from_dotpath(current_level, ".".join(keys))
    else:
        return current_level
