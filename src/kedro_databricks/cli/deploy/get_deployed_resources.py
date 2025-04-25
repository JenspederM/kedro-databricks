from __future__ import annotations

from collections import namedtuple
from collections.abc import MutableMapping

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import BaseJob
from kedro.framework.project import pipelines as kedro_pipelines
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.logger import get_logger
from kedro_databricks.utils import make_workflow_name

JobLink = namedtuple("JobLink", ["name", "url", "is_dev"])


log = get_logger("deploy").getChild("get_deployed_resources")


# TODO: Add tests
def get_deployed_resources(
    metadata: ProjectMetadata,
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
    w = WorkspaceClient()
    job_host = f"{w.config.host}/jobs"
    username = _get_username(_custom_username or w.current_user.me().user_name)
    log.info(f"Getting jobs for {username}")
    all_jobs = {
        job.settings.name: job
        for job in w.jobs.list()
        if job.settings is not None and job.settings.name is not None
    }
    jobs = _gather_user_jobs(metadata, all_jobs, pipelines, username, job_host)
    for job in jobs:
        if only_dev and not job.is_dev:  # pragma: no cover
            continue
        log.info(f"Run '{job.name}' at {job.url}")
    return jobs


# TODO: Add tests
def _gather_user_jobs(
    metadata: ProjectMetadata,
    all_jobs: dict[str, BaseJob],
    pipelines: MutableMapping,
    username: str,
    job_host: str,
) -> set[JobLink]:
    jobs = set()
    for job_name, job in all_jobs.items():
        is_dev = job_name.startswith("[dev")
        is_valid = _is_valid_job(metadata, pipelines, job_name)
        if (is_dev and username not in job_name) or not is_valid:  # pragma: no cover
            continue
        n = job_name.split(" - ")[0]
        link = JobLink(name=n, url=f"{job_host}/{job.job_id}", is_dev=is_dev)
        jobs.add(link)
    return jobs


def _is_valid_job(
    metadata: ProjectMetadata, pipelines: MutableMapping, job_name: str
) -> bool:
    return any(
        make_workflow_name(metadata.package_name, pipeline_name) in job_name
        for pipeline_name in pipelines
    )


def _get_username(username: str | None):
    if username is None:  # pragma: no cover
        raise ValueError("Could not get username from Databricks")
    if "@" in username:
        username = username.split("@")[0]
    return username
