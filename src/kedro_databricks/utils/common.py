from __future__ import annotations

import logging
import subprocess


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
