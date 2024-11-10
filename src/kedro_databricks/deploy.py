from __future__ import annotations

import logging
import os
import tarfile
from pathlib import Path

from databricks.sdk import WorkspaceClient
from kedro.framework.project import pipelines as kedro_pipelines
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.utils import make_workflow_name, run_cmd

_INVALID_CONFIG_MSG = """
No `databricks.yml` file found. Maybe you forgot to initialize the Databricks bundle?

You can initialize the Databricks bundle by running:

```
kedro databricks init
```
"""


class DeployController:
    def __init__(self, metadata: ProjectMetadata) -> None:
        self._msg = "Deploying to Databricks"
        self.metadata = metadata
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
        run_cmd(
            ["databricks", "fs", "mkdirs", f"dbfs:/FileStore/{self.package_name}"],
            msg=self._msg,
            warn=True,
        )

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
        run_cmd(
            [
                "databricks",
                "fs",
                "cp",
                "-r",
                "--overwrite",
                source_path.as_posix(),
                target_path,
            ],
            msg=self._msg,
        )
        self.log.info(f"{self._msg}: Data uploaded to {target_path}")

    def upload_project_config(self, conf: str):  # pragma: no cover
        """Upload the project configuration to DBFS.

        Args:
            conf (str): The conf folder.
        """
        conf_tar = self.project_path / f"dist/conf-{self.package_name}.tar.gz"
        with tarfile.open(conf_tar) as f:
            f.extractall("dist/", filter="tar")

        target_path = f"dbfs:/FileStore/{self.package_name}/{conf}"
        source_path = self.project_path / "dist" / conf
        if not source_path.exists():
            raise FileNotFoundError(f"Configuration path {source_path} does not exist")

        self.log.info(f"{self._msg}: Uploading configuration to {target_path}")
        run_cmd(
            [
                "databricks",
                "fs",
                "cp",
                "-r",
                "--overwrite",
                source_path.as_posix(),
                target_path,
            ],
            msg=self._msg,
        )
        self.log.info(f"{self._msg}: Configuration uploaded to {target_path}")

    def build_project(self):  # pragma: no cover
        """Build the project."""
        self.log.info(f"{self._msg}: Building the project")
        self.go_to_project()
        build_cmd = ["kedro", "package"]
        result = run_cmd(build_cmd, msg=self._msg)
        return result

    def deploy_project(self, target: str, debug: bool = False):
        """Deploy the project to Databricks.

        Args:
            target (str): Databricks target environment to deploy to.
            debug (bool): Whether to enable debug mode.
        """
        self.log.info(
            f"{self._msg}: Running `databricks bundle deploy --target {target}`"
        )
        deploy_cmd = ["databricks", "bundle", "deploy", "--target", target]
        if debug:
            deploy_cmd.append("--debug")
        run_cmd(deploy_cmd, msg=self._msg)
        self.log_deployed_resources()

    def log_deployed_resources(self, pipelines=kedro_pipelines) -> None:
        """Print the pipelines."""
        w = WorkspaceClient()

        user = w.current_user.me()
        job_host = f"{w.config.host}/jobs"
        username = user.user_name.split("@")[0]
        all_jobs = {job.settings.name: job for job in w.jobs.list()}
        jobs = []
        dev_jobs = set()
        for job_name, job in all_jobs.items():
            is_dev = job_name.startswith("[dev")
            for pipeline_name in pipelines:
                if make_workflow_name(self.package_name, pipeline_name) in job_name:
                    n = job_name.split(" - ")[0]
                    line = f"{n} - {job_host}/{job.job_id}"
                    if is_dev and username in job_name:
                        dev_jobs.add(line)
                    elif not is_dev:
                        jobs.add(line)

        if len(dev_jobs) > 0:
            _job_str = "\n\t".join(dev_jobs)
            self.log.info(
                f"{self._msg}: Successfully Deployed Development Jobs\n\t{_job_str}"
            )
        if len(jobs) > 0:
            _job_str = "\n\t".join(jobs)
            self.log.info(
                f"{self._msg}: Successfully Deployed Project Jobs\n\t{_job_str}"
            )


if __name__ == "__main__":

    class MetadataMock:
        def __init__(self, path: str, name: str):
            self.project_path = Path(path)
            self.project_name = name
            self.package_name = name
            self.source_dir = "src"
            self.env = "local"
            self.config_file = "conf/base"
            self.project_version = "0.16.0"
            self.project_description = "Test Project Description"
            self.project_author = "Test Author"
            self.project_author_email = "author@email.com"

    controller = DeployController(MetadataMock("/tmp", "fake_project"))

    controller.log_deployed_resources({"__default__": None})
