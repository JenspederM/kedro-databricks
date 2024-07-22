import logging
import subprocess
from typing import Any

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


def run_cmd(
    cmd: list[str], msg: str | None = None, warn: bool = False
) -> subprocess.CompletedProcess | None:
    """Run a shell command.

    Args:
        cmds (List[str]): list of commands to run
        msg (str, optional): message to raise if the command fails
        warn (bool): whether to log a warning if the command fails
    """

    if msg is None:
        msg = "Failed to run command"

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


def _null_check(x: Any) -> bool:
    """Check if a value is None or an empty dictionary.

    Args:
        x (Any): value to check

    Returns:
        bool: whether the value is None or an empty dictionary
    """
    return x is None or (isinstance(x, dict | list) and len(x) == 0)


def _remove_nulls_from_dict(d: dict[str, Any]) -> dict[str, float | int | str | bool]:
    """Remove None values from a dictionary.

    Args:
        d (Dict[str, Any]): dictionary to remove None values from

    Returns:
        Dict[str, float | int | str | bool]: dictionary with None values removed
    """

    for k, v in list(d.items()):
        if isinstance(v, dict):
            _remove_nulls_from_dict(v)
        elif isinstance(v, list):
            new_values = []
            for item in v:
                if isinstance(item, dict):
                    _remove_nulls_from_dict(item)

                if _null_check(item):
                    continue

                new_values.append(item)
            d[k] = new_values

        if _null_check(v):
            del d[k]

    return d
