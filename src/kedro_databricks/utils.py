from __future__ import annotations

import copy
import logging
import re
import shutil
import subprocess
from typing import IO, Any

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


class Command:
    def __init__(
        self, command: list[str], warn: bool = False, msg: str = "Error when running"
    ):
        self.log = logging.getLogger(self.__class__.__name__)
        self.command = command
        self.warn = warn
        self.msg = msg

    def __str__(self):
        return f"Command({self.command})"

    def __repr__(self):
        return self.__str__()

    def __rich_repr__(self):  # pragma: no cover
        yield "program", self.command[0]
        yield "args", self.command[1:]

    def run(self, *args):
        cmd = self.command + list(*args)
        self.log.info(f"Running command: {cmd}")
        with subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as popen:
            stdout = self._read(popen.stdout, self.log.info)
            stderr = self._read(popen.stderr, self.log.error)
            return_code = popen.wait()
            if return_code != 0:
                self._handle_error(stdout, stderr)

            return subprocess.CompletedProcess(
                args=cmd,
                returncode=return_code,
                stdout=stdout,
                stderr=stderr or "",
            )

    def _read(self, io: IO, log_func: Any) -> list[str]:
        lines = []
        while True:
            line = io.readline().decode("utf-8", errors="replace").strip()
            if not line:
                break
            log_func(f"{self}: {line}")
            lines.append(line)
        return lines

    def _handle_error(self, stdout: list[str], stderr: list[str]):
        error_msg = "\n".join(stderr)
        if not error_msg:  # pragma: no cover
            error_msg = "\n".join(stdout)
        if self.warn:
            self.log.warning(f"{self.msg} ({self.command}): {error_msg}")
        else:
            raise RuntimeError(f"{self.msg} ({self.command}): {error_msg}")


def make_workflow_name(package_name, pipeline_name: str) -> str:
    """Create a name for the Databricks workflow.

    Args:
        pipeline_name (str): The name of the pipeline

    Returns:
        str: The name of the workflow
    """
    if pipeline_name == "__default__":
        return package_name
    return f"{package_name}_{pipeline_name}"


def update_list(
    old: list[dict[str, Any]],
    new: list[dict[str, Any]],
    lookup_key: str,
    default: dict[str, Any] = {},
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
