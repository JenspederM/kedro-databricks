from __future__ import annotations

import copy
import logging
import re
import shutil
import subprocess
from typing import Any

from kedro import __version__ as kedro_version

KEDRO_VERSION = [int(x) for x in kedro_version.split(".")]
TASK_KEY_ORDER = [
    "task_key",
    "job_cluster_key",
    "new_cluster",
    "depends_on",
    "spark_python_task",
    "python_wheel_task",
]

WORKFLOW_KEY_ORDER = [
    "name",
    "tags",
    "access_control_list",
    "email_notifications",
    "schedule",
    "max_concurrent_runs",
    "job_clusters",
    "tasks",
]


def has_databricks_cli() -> bool:
    """Check if the Databricks CLI is installed."""
    return shutil.which("databricks") is not None


def get_entry_point(project_name: str) -> str:
    """Get the entry point for a project.

    Args:
        project_name (str): name of the project

    Returns:
        str: entry point for the project
    """
    entrypoint = project_name.strip().lower()
    entrypoint = re.sub(r" +", " ", entrypoint)
    return re.sub(r"[^a-zA-Z]", "-", entrypoint)


def require_databricks_run_script() -> bool:
    """Check if the current Kedro version is less than 0.19.8.

    Kedro 0.19.8 introduced a new `run_script` method that is required for
    running tasks on Databricks. This method is not available in earlier
    versions of Kedro. This function checks if the current Kedro version is
    less than 0.19.8.

    Returns:
        bool: whether the current Kedro version is less than 0.19.8
    """
    return KEDRO_VERSION < [0, 19, 8]


def run_cmd(
    cmd: list[str], msg: str = "Failed to run command", warn: bool = False
) -> subprocess.CompletedProcess | None:
    """Run a shell command.

    Args:
        cmds (List[str]): list of commands to run
        msg (str, optional): message to raise if the command fails
        warn (bool): whether to log a warning if the command fails
    """

    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, check=True)
        for line in result.stdout.decode().split("\n"):
            logging.info(line)
        return result
    except Exception as e:
        if warn:
            logging.warning(f"{msg}: {e}")
            return None
        else:
            raise Exception(f"{msg}: {e}")


def _sort_dict(d: dict[Any, Any], key_order: list[str]) -> dict[Any, Any]:
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


def _is_null_or_empty(x: Any) -> bool:
    """Check if a value is None or an empty dictionary.

    Args:
        x (Any): value to check

    Returns:
        bool: whether the value is None or an empty dictionary
    """
    return x is None or (isinstance(x, (dict, list)) and len(x) == 0)


def _remove_nulls_from_list(
    lst: list[dict | float | int | str | bool],
) -> list[dict | list]:
    """Remove None values from a list.

    Args:
        l (List[Dict[Any, Any]]): list to remove None values from

    Returns:
        List[Dict[Any, Any]]: list with None values removed
    """
    for i, item in enumerate(lst):
        value = remove_nulls(item)
        if _is_null_or_empty(value):
            lst.remove(item)
        else:
            lst[i] = value


def _remove_nulls_from_dict(
    d: dict[str, Any],
) -> dict[str, float | int | str | bool]:
    """Remove None values from a dictionary.

    Args:
        d (Dict[str, Any]): dictionary to remove None values from

    Returns:
        Dict[str, float | int | str | bool]: dictionary with None values removed
    """
    for k, v in list(d.items()):
        value = remove_nulls(v)
        if _is_null_or_empty(value):
            del d[k]
        else:
            d[k] = value
    return d


def remove_nulls(value: dict | list) -> dict | list:
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
