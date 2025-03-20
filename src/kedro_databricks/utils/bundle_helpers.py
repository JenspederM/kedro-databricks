from __future__ import annotations

import copy
import re
from typing import Any

from kedro_databricks.constants import KEDRO_VERSION


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
    entrypoint = re.sub(r"[^a-zA-Z]", "-", entrypoint)
    entrypoint = re.sub(r"(-+)$", "", entrypoint)
    entrypoint = re.sub(r"^(-+)", "", entrypoint)
    return entrypoint


def require_databricks_run_script(_version=KEDRO_VERSION) -> bool:
    """Check if the current Kedro version is less than 0.19.8.

    Kedro 0.19.8 introduced a new `run_script` method that is required for
    running tasks on Databricks. This method is not available in earlier
    versions of Kedro. This function checks if the current Kedro version is
    less than 0.19.8.

    Returns:
        bool: whether the current Kedro version is less than 0.19.8
    """
    return _version < [0, 19, 8]


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
