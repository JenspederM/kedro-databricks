from __future__ import annotations

import logging
import re
import subprocess
from typing import Any

from kedro_databricks.constants import KEDRO_VERSION


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
    def __init__(self, command: list[str], warn: bool = False, msg: str = ""):
        if msg is None:  # pragma: no cover
            msg = f'Executing ({" ".join(command)})'
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

    def _read_stdout(self, process: subprocess.Popen):
        stdout = []
        while True:
            line = process.stdout.readline()  # type: ignore - we know it's there
            if not line and process.poll() is not None:
                break
            print(line, end="")  # noqa: T201
            stdout.append(line)
        return stdout

    def _run_command(self, command, **kwargs):
        """Run a command while printing the live output"""
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            **kwargs,
        )
        stdout = self._read_stdout(process)
        process.stdout.close()  # type: ignore - we know it's there
        process.wait()
        if process.returncode != 0 and "deploy" not in command:
            self._handle_error()
        return subprocess.CompletedProcess(
            args=command,
            returncode=process.returncode,
            stdout=stdout or [""],
            stderr=[],
        )

    def run(self, *args):
        cmd = self.command + list(*args)
        self.log.info(f"Running command: {cmd}")
        return self._run_command(cmd)

    def _handle_error(self):
        error_msg = f"{self.msg}: Failed to run command - `{' '.join(self.command)}`"
        if self.warn:
            self.log.warning(error_msg)
        else:
            raise RuntimeError(error_msg)


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
