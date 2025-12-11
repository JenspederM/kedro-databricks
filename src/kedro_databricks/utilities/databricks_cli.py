import json
import re
import shutil
import subprocess
import time
from pathlib import Path

import yaml
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.constants import (
    DEFAULT_ENV,
    MINIMUM_DATABRICKS_VERSION,
)
from kedro_databricks.utilities.common import get_arg_value, version_to_str
from kedro_databricks.utilities.logger import get_logger


class DatabricksCli:
    """Databricks CLI command collection."""

    def __init__(
        self,
        metadata: ProjectMetadata,
        env: str = DEFAULT_ENV,
        additional_args: list[str] | None = None,
    ):
        """Initialize the Databricks CLI command collection.

        Args:
            additional_args (list[str] | None): Additional arguments to be passed to the
                `databricks` CLI.
        """
        self.log = get_logger("databricks_cli")
        if additional_args is None:
            additional_args = []
        self.metadata = metadata
        self.env = env
        self.args = additional_args
        self._check_self(warn=False)

    def version(self, warn=True):
        result = self._run_command(["databricks", "--version"], warn=warn, silent=True)
        self._check_result(
            result,
            "Failed to get Databricks CLI version",
            pass_when_includes="Databricks CLI",
        )
        stdout = "\n".join(result.stdout).strip()
        version_str = re.sub(r".*(v\d+\.\d+\.\d+)", r"\1", stdout)
        return list(map(int, version_str[1:].split(".")))

    def validate(self):
        cmd = ["databricks", "bundle", "validate", "--output", "json"] + self.args
        result = self._run_command(
            cmd, warn=True, cwd=self.metadata.project_path, silent=True
        )
        self._check_result(result, "Failed to validate Databricks Asset Bundle")
        try:
            # Skip any warnings before the JSON output
            first_bracket = next(
                i
                for i, line in enumerate(result.stdout)
                if line.strip().startswith("{")
            )

            result.stdout = result.stdout[first_bracket:]
            return json.loads("".join(result.stdout))
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Failed to parse Databricks Asset Bundle validation output\n{''.join(result.stdout)}"
            ) from exc

    def init(self, assets_dir: Path, template_params: Path):
        cmd = [
            "databricks",
            "bundle",
            "init",
            assets_dir.as_posix(),
            "--config-file",
            template_params.as_posix(),
            "--output-dir",
            self.metadata.project_path.as_posix(),
        ] + list(self.args)
        result = self._run_command(cmd, warn=True, cwd=self.metadata.project_path)
        self._check_result(result, "Failed to initialize Databricks Asset Bundle")
        shutil.rmtree(assets_dir)
        max_wait_seconds = 10
        while not (self.metadata.project_path / "databricks.yml").exists():
            max_wait_seconds -= 1
            if max_wait_seconds <= 0:
                raise RuntimeError("Databricks Asset Bundle initialization timed out.")
            time.sleep(1)
        return self.validate()

    def deploy(self):
        cmd = (
            [
                "databricks",
                "bundle",
                "deploy",
            ]
            + list(self.args)
            + self._get_default_target()
        )
        result = self._run_command(cmd, warn=True, cwd=self.metadata.project_path)
        self._check_result(
            result,
            "Failed to deploy Databricks Asset Bundle",
            pass_when_includes="Deployment complete!\n",
        )

    def summary(self):
        cmd = (
            [
                "databricks",
                "bundle",
                "summary",
            ]
            + list(self.args)
            + self._get_default_target()
        )
        result = self._run_command(cmd, warn=True, cwd=self.metadata.project_path)
        self._check_result(result, "Failed to summarize Databricks Asset Bundle")

    def upload(self):
        source_path = self.metadata.project_path / "data"
        if not source_path.exists():
            self.log.warning(f"'{source_path}' does not exist. Skipping upload.")
            return
        file_path = self._get_env_file_path()
        if not file_path:
            self.log.warning(
                f"No file path found for the given environment: '{self.env}'"
            )
            return
        target_path = f"dbfs:{file_path}/data"
        cmd = (
            [
                "databricks",
                "fs",
                "cp",
                "-r",
                "--overwrite",
                source_path.as_posix(),
                target_path,
            ]
            + self.args
            + self._get_default_target()
        )
        result = self._run_command(cmd, cwd=self.metadata.project_path)
        self._check_result(result, "Failed to upload data")

    def run(self, pipeline: str):
        cmd = (
            ["databricks", "bundle", "run", pipeline]
            + list(self.args)
            + self._get_default_target()
        )
        result = self._run_command(cmd, warn=True, cwd=self.metadata.project_path)
        self._check_result(result, "Failed to run Databricks job")

    def destroy(self):
        cmd = (
            ["databricks", "bundle", "destroy"]
            + list(self.args)
            + self._get_default_target()
        )
        result = self._run_command(cmd, warn=True, cwd=self.metadata.project_path)
        self._check_result(result, "Failed to destroy Databricks resources")

    def _check_self(self, warn=False):
        if not shutil.which("databricks"):
            error_msg = (
                "Databricks CLI is not installed or not found in PATH. "
                "Please install it from "
                "https://docs.databricks.com/en/dev-tools/cli/install.html"
            )
            raise RuntimeError(error_msg)
        return self._check_version(warn=warn)

    def _check_version(self, warn=False):
        current_databricks_version = self.version(warn=warn)
        if current_databricks_version < MINIMUM_DATABRICKS_VERSION:
            error_msg = f"""{version_to_str(current_databricks_version)} < {version_to_str(MINIMUM_DATABRICKS_VERSION)}
        Your Databricks CLI version is {version_to_str(current_databricks_version)},
        but this script requires at least {version_to_str(MINIMUM_DATABRICKS_VERSION)}.
        Visit https://docs.databricks.com/en/dev-tools/cli/install.html to install the latest version.
            """
            raise RuntimeError(error_msg)

    def _get_default_target(self):
        target = get_arg_value(self.args, "--target")
        if target is None:
            return ["--target", self.env]
        return []

    def _check_result(
        self, result: subprocess.CompletedProcess, msg: str, pass_when_includes=None
    ):
        if result.returncode != 0 and not (
            pass_when_includes is not None
            and pass_when_includes in "".join(result.stdout)
        ):  # pragma: no cover
            err = "\n".join(result.stdout)
            raise RuntimeError(f"({result.returncode}) {msg}\n{err}")

    def _read_stdout(self, process: subprocess.Popen, silent=False):
        stdout = []
        while True:
            line = process.stdout.readline()  # type: ignore - we know it's there
            if not line and process.poll() is not None:
                break
            if not silent:
                print(line, end="")  # noqa: T201
            stdout.append(line)
        return stdout

    def _run_command(self, command, warn=False, silent=False, **kwargs):
        """Run a command while printing the live output"""
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            **kwargs,
        )
        stdout = self._read_stdout(process, silent=silent)
        process.stdout.close()  # type: ignore - we know it's there
        result = subprocess.CompletedProcess(
            args=command,
            returncode=process.returncode,
            stdout=stdout or [""],
            stderr=[],
        )
        return result

    def _get_env_file_path(self):
        """Get the file path for the given environment from the catalog.yml file.

        Args:
            metadata (ProjectMetadata): The project metadata.
            env (str): The environment to get the file path for.

        Returns:
            str | None: The file path for the given environment, or None if not found.
        """
        target_catalog = self.metadata.project_path / "conf" / self.env / "catalog.yml"
        if not target_catalog.exists():
            self.log.warning(
                f"Catalog file {target_catalog.relative_to(self.metadata.project_path)} not found"
            )
            return
        yaml_content = yaml.safe_load(target_catalog.read_text())
        file_path = yaml_content.get("_file_path")
        if not file_path:
            return
        return file_path
