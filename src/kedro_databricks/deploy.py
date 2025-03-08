from __future__ import annotations

import logging
import os
import tarfile
from collections import namedtuple
from collections.abc import MutableMapping
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import BaseJob
from kedro.framework.project import pipelines as kedro_pipelines
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.constants import DEFAULT_TARGET, INVALID_CONFIG_MSG
from kedro_databricks.utils.common import Command, make_workflow_name

JobLink = namedtuple("JobLink", ["name", "url", "is_dev"])


class DeployController:
    def __init__(self, metadata: ProjectMetadata, env: str = DEFAULT_TARGET) -> None:
        self._msg = "Deploying to Databricks"
        self.package_name: str = metadata.package_name
        self.project_path: Path = metadata.project_path
        self.log: logging.Logger = logging.getLogger(metadata.package_name)
        self.env = env

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
            raise FileNotFoundError(INVALID_CONFIG_MSG)
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

    def _untar_conf(self, conf: str):
        dist_dir = self.project_path / "dist"
        conf_tar = dist_dir / f"conf-{self.package_name}.tar.gz"

        if not conf_tar.exists():  # pragma: no cover
            self.log.error("No files found")
            dist_files = list(dist_dir.rglob("*"))
            raise FileNotFoundError(
                f"Configuration tar file {conf_tar} does not exist - {dist_files}"
            )

        with tarfile.open(conf_tar) as tar:
            file_names = tar.getnames()
            for _file in file_names:
                if _file.startswith("."):  # pragma: no cover - hidden files
                    continue
                tar.extract(_file, dist_dir)

        source_dir = dist_dir / conf
        if not source_dir.exists():  # pragma: no cover
            dist_files = list(dist_dir.rglob("*"))
            raise FileNotFoundError(
                f"Configuration path {source_dir} does not exist - {dist_files}"
            )

        return source_dir

    def upload_project_config(self, conf: str):  # pragma: no cover
        """Upload the project configuration to DBFS.

        Args:
            conf (str): The conf folder.
        """
        target_path = f"dbfs:/FileStore/{self.package_name}/{conf}"
        source_path = self._untar_conf(conf)
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

    def _check_result(self, messages) -> bool:
        return messages and "Deployment complete!" in messages[-1]

    def deploy_project(self, databricks_args: list[str]):  # pragma: no cover
        """Deploy the project to Databricks.

        Args:
            databricks_args (list[str]): Databricks arguments.

        Returns:
            subprocess.CompletedProcess: The result of the deployment.
        """
        deploy_cmd = ["databricks", "bundle", "deploy"] + databricks_args
        target = self._get_target(databricks_args)
        if target is None:
            deploy_cmd += ["--target", self.env]
        self.log.info(f"{self._msg}: Running `{' '.join(deploy_cmd)}`")
        result = Command(deploy_cmd, msg=self._msg, warn=True).run()
        # databricks bundle deploy logs to stderr for some reason.
        if self._check_result(result.stdout) or self._check_result(result.stderr):
            result.returncode = 0
        self.log.info(f"{self._msg}: Successfully Deployed Jobs")
        self.log_deployed_resources(only_dev=target in ["dev", "local"])
        return result

    def _get_target(self, args: list[str]):
        for i, arg in enumerate(args):
            if arg == "--target":
                return args[i + 1]

    def _get_username(self, w: WorkspaceClient, _custom_username: str | None):
        username = _custom_username or w.current_user.me().user_name
        if username is None:  # pragma: no cover
            raise ValueError("Could not get username from Databricks")
        if "@" in username:
            username = username.split("@")[0]
        return username

    def log_deployed_resources(
        self,
        pipelines: MutableMapping = kedro_pipelines,
        only_dev: bool = False,
        _custom_username: str | None = None,
    ) -> set[JobLink]:
        """Print deployed pipelines.

        Args:
            pipelines (_ProjectPipelines): Project pipelines.
            only_dev (bool): Whether to show only dev jobs.
            _custom_username (str): Custom username to use.

        Returns:
            dict[str, set[str]]: Deployed pipelines.
        """
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
        username = self._get_username(w, _custom_username)
        self.log.info(f"{self._msg}: Getting jobs for {username}")
        all_jobs = {
            job.settings.name: job
            for job in w.jobs.list()
            if job.settings is not None and job.settings.name is not None
        }
        jobs = self._gather_user_jobs(all_jobs, pipelines, username, job_host)
        for job in jobs:
            if only_dev and not job.is_dev:  # pragma: no cover
                continue
            self.log.info(f"Run '{job.name}' at {job.url}")
        return jobs

    def _gather_user_jobs(
        self,
        all_jobs: dict[str, BaseJob],
        pipelines: MutableMapping,
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

    def _is_valid_job(self, pipelines: MutableMapping, job_name: str) -> bool:
        return any(
            make_workflow_name(self.package_name, pipeline_name) in job_name
            for pipeline_name in pipelines
        )
