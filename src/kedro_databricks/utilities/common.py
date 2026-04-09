from __future__ import annotations

import copy
import re
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
