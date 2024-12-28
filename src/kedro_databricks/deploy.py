from __future__ import annotations

import logging
import os
import tarfile
from collections import namedtuple
from pathlib import Path

from databricks.sdk import WorkspaceClient
from kedro.framework.project import _ProjectPipelines
from kedro.framework.project import pipelines as kedro_pipelines
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.utils import Command, make_workflow_name

_INVALID_CONFIG_MSG = """
No `databricks.yml` file found. Maybe you forgot to initialize the Databricks bundle?

You can initialize the Databricks bundle by running:

```
kedro databricks init
```
"""

JobLink = namedtuple("JobLink", ["name", "url", "is_dev"])


class DeployController:
    def __init__(self, metadata: ProjectMetadata) -> None:
        self._msg = "Deploying to Databricks"
        self.package_name = metadata.package_name
        self.project_path = metadata.project_path
        self.log = logging.getLogger(metadata.package_name)

    def go_to_project(self) -> Path:
        """Change the current working directory to the project path.

        Returns:
            pathlib.Path: Path to the project directory.

        Raises:
            FileNotFoundError: If the project path does not exist.
        """
        project_path = Path(self.project_path)
        if not project_path.exists():
            raise FileNotFoundError(f"Project path {project_path} does not exist")
        os.chdir(project_path)
        return project_path

    def validate_databricks_config(self):
        """Check if the Databricks configuration file exists.

        Returns:
            bool: Whether the Databricks configuration file exists.

        Raises:
            FileNotFoundError: If the Databricks configuration file does not exist.
        """
        if not (self.project_path / "databricks.yml").exists():
            raise FileNotFoundError(_INVALID_CONFIG_MSG)
        return True

    def create_dbfs_dir(self):  # pragma: no cover
        """Create a directory in DBFS."""
        cmd = ["databricks", "fs", "mkdirs", f"dbfs:/FileStore/{self.package_name}"]
        Command(cmd, msg=self._msg, warn=True).run()

    def upload_project_data(self):  # pragma: no cover
        """Upload the project data to DBFS.

        Args:
            metadata (ProjectMetadata): Project metadata.
            MSG (str): Message to display.
        """
        target_path = f"dbfs:/FileStore/{self.package_name}/data"
        source_path = self.project_path / "data"
        if not source_path.exists():
            self.log.warning(f"Data path {source_path} does not exist")
            return

        self.log.info(
            f"{self._msg}: Uploading {source_path.relative_to(self.project_path)} to {target_path}"
        )
        cmd = [
            "databricks",
            "fs",
            "cp",
            "-r",
            "--overwrite",
            source_path.as_posix(),
            target_path,
        ]
        result = Command(cmd, msg=self._msg).run()
        self.log.info(f"{self._msg}: Data uploaded to {target_path}")
        return result

    def upload_project_config(self, conf: str):  # pragma: no cover
        """Upload the project configuration to DBFS.

        Args:
            conf (str): The conf folder.
        """
        target_path = f"dbfs:/FileStore/{self.package_name}/{conf}"
        source_path = self.project_path / "dist" / conf
        conf_tar = self.project_path / f"dist/conf-{self.package_name}.tar.gz"

        if not conf_tar.exists():
            self.log.error("No files found")
            raise FileNotFoundError(f"Configuration tar file {conf_tar} does not exist")

        with tarfile.open(conf_tar) as tar:
            file_names = tar.getnames()
            for _file in file_names:
                tar.extract(_file, source_path)

        if not source_path.exists():
            raise FileNotFoundError(f"Configuration path {source_path} does not exist")

        self.log.info(f"{self._msg}: Uploading configuration to {target_path}")
        cmd = [
            "databricks",
            "fs",
            "cp",
            "-r",
            "--overwrite",
            source_path.as_posix(),
            target_path,
        ]
        result = Command(cmd, msg=self._msg).run()
        self.log.info(f"{self._msg}: Configuration uploaded to {target_path}")
        return result

    def build_project(self):  # pragma: no cover
        """Build the project."""
        self.log.info(f"{self._msg}: Building the project")
        self.go_to_project()
        build_cmd = ["kedro", "package"]
        result = Command(build_cmd, msg=self._msg).run()
        return result

    def deploy_project(self, target: str, debug: bool = False, var: list[str] = []):
        """Deploy the project to Databricks.

        Args:
            target (str): Databricks target environment to deploy to.
            debug (bool): Whether to enable debug mode.
            variables (list[str]): List of variables to set.
        """
        self.log.info(
            f"{self._msg}: Running `databricks bundle deploy --target {target}`"
        )
        _var = [_v for v in var for _v in ["--var", v]]
        deploy_cmd = ["databricks", "bundle", "deploy", "--target", target, *_var]
        if debug:
            deploy_cmd.append("--debug")
        result = Command(deploy_cmd, msg=self._msg, warn=True).run()
        # databricks bundle deploy logs to stderr for some reason.
        if (result.stderr and "Deployment complete!" in result.stderr[-1]) or (
            result.stdout and "Deployment complete!" in result.stdout[-1]
        ):
            result.returncode = 0
        self.log.info(f"{self._msg}: Successfully Deployed Jobs")
        self.log_deployed_resources(only_dev=target in ["dev", "local"])
        return result

    def log_deployed_resources(
        self,
        pipelines: _ProjectPipelines = kedro_pipelines,
        only_dev: bool = False,
        _custom_username: str | None = None,
    ) -> dict[str, set[str]]:
        """Print the pipelines."""
        w = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            account_id=os.getenv("DATABRICKS_ACCOUNT_ID"),
            username=os.getenv("DATABRICKS_USERNAME"),
            password=os.getenv("DATABRICKS_PASSWORD"),
            client_id=os.getenv("DATABRICKS_CLIENT_ID"),
            client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
            token=os.getenv("DATABRICKS_TOKEN"),
            profile=os.getenv("DATABRICKS_PROFILE"),
            config_file=os.getenv("DATABRICKS_CONFIG_FILE"),
        )
        job_host = f"{w.config.host}/jobs"
        username = _custom_username or w.current_user.me().user_name.split("@")[0]
        all_jobs = {job.settings.name: job for job in w.jobs.list()}
        jobs = self._gather_user_jobs(all_jobs, pipelines, username, job_host)
        for job in jobs:
            if only_dev and not job.is_dev:
                continue
            self.log.info(f"Run '{job.name}' at {job.url}")
        return jobs

    def _gather_user_jobs(
        self,
        all_jobs: dict[str, str],
        pipelines: _ProjectPipelines,
        username,
        job_host,
    ) -> set[JobLink]:
        jobs = set()
        for job_name, job in all_jobs.items():
            is_dev = job_name.startswith("[dev")
            is_valid = self._is_valid_job(pipelines, job_name)
            if (
                is_dev and username not in job_name
            ) or not is_valid:  # pragma: no cover
                continue
            n = job_name.split(" - ")[0]
            link = JobLink(name=n, url=f"{job_host}/{job.job_id}", is_dev=is_dev)
            jobs.add(link)
        return jobs

    def _is_valid_job(self, pipelines: _ProjectPipelines, job_name: str) -> bool:
        return any(
            make_workflow_name(self.package_name, pipeline_name) in job_name
            for pipeline_name in pipelines
        )
