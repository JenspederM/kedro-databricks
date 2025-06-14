from __future__ import annotations

import logging
import re
import shutil
import subprocess
from pathlib import Path

import yaml

from kedro_databricks.constants import KEDRO_VERSION, MINIMUM_DATABRICKS_VERSION
from kedro_databricks.logger import get_logger

log = get_logger("utils")


class Command:
    """A class to run shell commands and handle their output.

    This class provides a way to run shell commands while capturing their output
    and logging it. It can also handle errors by either raising an exception or
    logging a warning, depending on the `warn` parameter.

    Attributes:
        command (list[str]): The command to run as a list of strings.
        log (logging.Logger): The logger to use for logging output.
        warn (bool): If True, log a warning instead of raising an exception on error.

    Args:
        command (list[str]): The command to run as a list of strings.
        log (logging.Logger): The logger to use for logging output.
        warn (bool): If True, log a warning instead of raising an exception on error.
    """

    def __init__(self, command: list[str], log: logging.Logger, warn: bool = False):
        self.log = log.getChild("command")
        self.command = command
        self.warn = warn

    def __str__(self):  # pragma: no cover
        return f"Command({self.command})"

    def __repr__(self):  # pragma: no cover
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
        result = subprocess.CompletedProcess(
            args=command,
            returncode=process.returncode,
            stdout=stdout or [""],
            stderr=[],
        )
        if result.returncode != 0 and "deploy" not in command:
            self._handle_error(result)
        return result

    def run(self, *args, **kwargs):
        cmd = self.command + list(*args)
        self.log.info(f"Running command: {cmd}")
        return self._run_command(cmd, **kwargs)

    def _handle_error(self, result: subprocess.CompletedProcess):
        stdout = "\n".join(result.stdout)
        error_msg = f"Failed to run command `{' '.join(self.command)}`:\n\n{stdout}"
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


def read_databricks_config(project_path: Path) -> dict:
    """Read the databricks.yml configuration file.

    Args:
        project_path (Path): The path to the Kedro project.

    Returns:
        dict: The configuration as a dictionary.
    """
    with open(project_path / "databricks.yml") as f:
        conf = yaml.safe_load(f)
    return conf


def get_bundle_name(config: dict) -> str:
    """Get the bundle name from the databricks.yml configuration.

    Args:
        config (dict): The configuration as a dictionary.

    Returns:
        str: The bundle name.

    Raises:
        ValueError: If the bundle name is not found.
    """
    bundle_name = config.get("bundle", {}).get("name")
    if bundle_name is None:
        raise ValueError("No bundle name found in databricks.yml")
    return bundle_name


def get_targets(config: dict) -> dict:
    """Get the targets from the databricks.yml configuration.

    Args:
        config (dict): The configuration as a dictionary.

    Returns:
        dict: The targets as a dictionary.

    Raises:
        ValueError: If the targets are not found.
    """
    targets = config.get("targets")
    if targets is None:
        raise ValueError("No targets found in databricks.yml")
    return targets


def assert_databricks_cli(raise_error: bool = True):  # pragma: no cover
    """Check if the Databricks CLI is installed.

    Raises:
        RuntimeError: If the Databricks CLI is not installed or the wrong version is used.
        RuntimeError: If the Databricks CLI version is less than the minimum required version.
    """
    if shutil.which("databricks") is None:  # pragma: no cover
        msg = "Databricks CLI is not installed."
        if raise_error:
            raise RuntimeError(msg)
        return msg
    return _check_version(raise_error)


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


def _check_version(raise_error: bool = True):
    current_databricks_version = _get_databricks_cli_version()
    if current_databricks_version < MINIMUM_DATABRICKS_VERSION:
        error_msg = f"""{_version_to_str(current_databricks_version)} < {_version_to_str(MINIMUM_DATABRICKS_VERSION)}
    Your Databricks CLI version is {_version_to_str(current_databricks_version)},
    but this script requires at least {_version_to_str(MINIMUM_DATABRICKS_VERSION)}.
    Visit https://docs.databricks.com/en/dev-tools/cli/install.html to install the latest version.
        """
        if raise_error:
            raise RuntimeError(error_msg)
        return error_msg


def _get_databricks_cli_version() -> list[int]:
    result = Command(["databricks", "--version"], log, warn=True).run()
    stdout = "\n".join(result.stdout).strip()
    version_str = re.sub(r".*(\d+\.\d+\.\d+)", r"\1", stdout)
    return list(map(int, version_str.split(".")))


def _version_to_str(version: list[int]) -> str:
    if len(version) != 3:  # noqa: PLR2004 - Semantic versioning requires 3 parts
        raise ValueError(f"Invalid version: {version}")
    return ".".join(str(x) for x in version)
