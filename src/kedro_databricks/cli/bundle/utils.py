from __future__ import annotations

import copy
import re
from typing import Any

from kedro.pipeline.node import Node

from kedro_databricks.constants import MAX_TASK_KEY_LENGTH
from kedro_databricks.logger import get_logger

log = get_logger("bundle").getChild(__name__)


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


def sort_dict(d: dict[Any, Any], key_order: list[str]) -> dict[Any, Any]:
    """Recursively sort the keys of a dictionary.

    Args:
        d (Dict[Any, Any]): dictionary to sort
        key_order (List[str]): list of keys to sort by

    Returns:
        Dict[Any, Any]: dictionary with ordered values
    """
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
