from __future__ import annotations

import json
import shutil
import subprocess
import time
from collections.abc import Callable

import yaml
from click.testing import CliRunner
from kedro.framework.startup import ProjectMetadata
from kedro.pipeline import Pipeline, node

from kedro_databricks.commands.init import _get_targets, _read_databricks_config
from kedro_databricks.constants import DEFAULT_ENV
from kedro_databricks.plugin import commands
from kedro_databricks.utilities.common import require_databricks_run_script


def init_project(metadata: ProjectMetadata, cli_runner: CliRunner):
    init_cmd = ["databricks", "init"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)
    assert metadata.project_path.exists(), "Project path not created"
    assert metadata.project_path.is_dir(), "Project path is not a directory"
    assert metadata.project_path / "databricks.yml", "Databricks config not created"

    databricks_config = _read_databricks_config(metadata.project_path)
    targets = _get_targets(databricks_config)
    for target in targets:
        override_path = metadata.project_path / "conf" / target / "databricks.yml"
        assert override_path.exists(), (
            f"Resource Overrides at {override_path} does not exist"
        )


def bundle_project(metadata: ProjectMetadata, cli_runner: CliRunner):
    bundle_cmd = ["databricks", "bundle", "--env", DEFAULT_ENV]
    result = cli_runner.invoke(commands, bundle_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)


def deploy_project(metadata: ProjectMetadata, cli_runner: CliRunner):
    deploy_cmd = ["databricks", "deploy"]
    result = cli_runner.invoke(commands, deploy_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)


def destroy_project(metadata: ProjectMetadata, cli_runner: CliRunner):
    destroy_cmd = ["databricks", "destroy", "--", "--auto-approve"]
    result = cli_runner.invoke(commands, destroy_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)
    wait_for_job_deletion([])
    wait_for_volume_deletion(metadata)


def reset_project(metadata: ProjectMetadata):
    reset_init(metadata)
    reset_bundle(metadata)


def reset_init(metadata: ProjectMetadata):
    (metadata.project_path / "databricks.yml").unlink(missing_ok=True)
    shutil.rmtree(metadata.project_path / "conf" / "dev", ignore_errors=True)
    shutil.rmtree(metadata.project_path / "conf" / "prod", ignore_errors=True)


def reset_bundle(metadata):
    """Reset the bundle to its initial state."""
    bundle_dir = metadata.project_path / "resources"
    shutil.rmtree(bundle_dir, ignore_errors=True)


def wait_for_job_deletion(args) -> None:
    def _gather_jobs():
        jobs = subprocess.run(  # noqa: F821
            ["databricks", "jobs", "list", "--output", "json"] + args,
            check=True,
            capture_output=True,
            text=True,
        )
        if jobs.stdout:
            job_list = json.loads(jobs.stdout)
        else:
            job_list = []
        return [
            {
                "job_id": job["job_id"],
                "name": job["settings"]["name"],
            }
            for job in job_list
            if "develop_eggs" in job["settings"]["name"]
        ]

    def _delete_jobs(job_list):
        for j in job_list:
            res = subprocess.run(
                ["databricks", "jobs", "delete", str(j["job_id"])] + args,
                check=False,
                capture_output=True,
                text=True,
            )
            if res.returncode != 0:
                raise RuntimeError(f"Failed to delete job {j['name']}")

    job_list = _gather_jobs()
    max_tries = 12
    while job_list:
        max_tries -= 1
        if max_tries <= 0:
            raise TimeoutError("Timed out waiting for job deletion.")
        job_list = _gather_jobs()
        if job_list:
            _delete_jobs(job_list)
            time.sleep(5)
    print("All jobs have been deleted.")


def wait_for_volume_deletion(metadata: ProjectMetadata) -> None:
    resources_path = metadata.project_path / "resources"
    package_name = metadata.package_name
    volume_resources = list(resources_path.rglob("volumes.*.yml"))
    if not volume_resources:
        print("No volume resources found. Skipping volume deletion.")
        return
    elif len(volume_resources) > 1:
        raise ValueError("Multiple volume resource files found.")
    volume_path = resources_path / volume_resources[0].name
    with open(volume_path) as f:
        volume_resource = yaml.safe_load(f)
    volume_catalog = (
        volume_resource.get("resources", {})
        .get("volumes", {})
        .get(f"{package_name}_volume", {})
        .get("catalog_name")
    )
    volume_schema = (
        volume_resource.get("resources", {})
        .get("volumes", {})
        .get(f"{package_name}_volume", {})
        .get("schema_name")
    )
    volume_name = (
        volume_resource.get("resources", {})
        .get("volumes", {})
        .get(f"{package_name}_volume", {})
        .get("name")
    )
    if not volume_catalog or not volume_schema or not volume_name:
        raise ValueError("Volume resource information is incomplete.")
    volume_full_name = f"{volume_catalog}.{volume_schema}.{volume_name}"
    volume_exists = True
    max_tries = 12
    while volume_exists:
        max_tries -= 1
        if max_tries <= 0:
            raise TimeoutError("Timed out waiting for volume deletion.")
        volume_exists = (
            subprocess.run(
                ["databricks", "volumes", "read", volume_full_name, "--output", "json"],
                check=False,
                capture_output=True,
                text=True,
            ).stdout.strip()
            and True
            or False
        )
        if volume_exists:
            subprocess.run(
                ["databricks", "volumes", "delete", volume_full_name],
                check=True,
            )
            time.sleep(5)
    print(f"Volume {volume_full_name} has been deleted.")


def validate_bundle(
    metadata: ProjectMetadata,
    env: str,
    required_files: list[str],
    task_validator: Callable,
):
    """Validate the resources generated by the bundle command."""
    resources_dir = metadata.project_path / "resources"
    assert resources_dir.exists(), "Resources directory not created"
    assert resources_dir.is_dir(), "Resources directory is not a directory"
    resource_files = list(resources_dir.iterdir())
    extra_files = set(f.name for f in resource_files) - set(required_files)
    assert not extra_files, f"Unexpected resource files found: {extra_files}"
    for file in resources_dir.iterdir():
        assert file.is_file(), f"{file} is not a file"
        assert file.suffix in (".yml", ".yaml"), f"{file} is not a YAML file"
        assert file.name in required_files, f"{file} is not a required file"
        with open(file) as f:
            resource = yaml.safe_load(f)
        assert "targets" in resource, f"'targets' key not found in {file}"
        assert env in resource["targets"], f"Environment '{env}' not found in {file}"
        resource = resource["targets"][env]
        assert "resources" in resource, f"'resources' key not found in {file}"
        if file.name.startswith("jobs."):
            assert len(resource["resources"]["jobs"]) == 1, (
                f"Expected 1 job in {file}, found {len(resource['resources']['jobs'])}"
            )
            job_name = file.name.split(".")[1]
            job = resource["resources"]["jobs"].get(job_name)
            assert job is not None, f"Job {job_name} not found in {file}"
            tasks = job.get("tasks")
            assert tasks is not None, f"Tasks not found in job {file}"
            task_validator(tasks)


def identity(arg):
    return arg


def long_identity(*args):
    return "_".join(str(arg) for arg in args)


pipeline = Pipeline(
    [
        node(
            identity,
            ["input"],
            ["intermediate"],
            name="node0",
            tags=["tag0", "tag1"],
        ),
        node(identity, ["intermediate"], ["output"], name="node1"),
        node(identity, ["intermediate"], ["output2"], name="node2", tags=["tag0"]),
        node(
            identity,
            ["intermediate"],
            ["output3"],
            name="node3",
            tags=["tag1", "tag2"],
        ),
        node(identity, ["intermediate"], ["output4"], name="node4", tags=["tag2"]),
    ]
)


def _generate_task(
    task_key: int | str,
    depends_on: list[str] = [],
    conf: str = "conf",
    runtime_params: str = "",
):
    entry_point = "fake-project"
    params = [
        "--nodes",
        task_key,
        "--conf-source",
        "/${workspace.file_path}/" + conf,
        "--env",
        "${var.environment}",
    ]

    if require_databricks_run_script():
        entry_point = "databricks_run"
        params = params + ["--package-name", "fake_project"]

    if runtime_params:
        params = params + ["--params", runtime_params]

    task = {
        "task_key": task_key,
        "python_wheel_task": {
            "package_name": "fake_project",
            "entry_point": entry_point,
            "parameters": params,
        },
    }

    if len(depends_on) > 0:
        task["depends_on"] = [{"task_key": dep} for dep in depends_on]

    return task


def generate_job(conf="conf"):
    tasks = []

    for i in range(5):
        if i == 0:
            depends_on = []
        else:
            depends_on = ["node0"]
        tasks.append(_generate_task(f"node{i}", depends_on, conf))

    return {
        "name": "job1",
        "tasks": tasks,
    }


JOB = generate_job()

DEFAULT_PIPELINE_REGISTRY = """
from kedro.pipeline import Pipeline, node


def identity(arg):
    return arg


def register_pipelines():
    pipeline = Pipeline(
        [
            node(identity, ["input"], ["intermediate"], name="node0", tags=["tag0", "tag1"]),
            node(identity, ["intermediate"], ["output"], name="node1"),
            node(identity, ["intermediate"], ["output2"], name="node2", tags=["tag0"]),
            node(identity, ["intermediate"], ["output3"], name="node3", tags=["tag1", "tag2"]),
            node(identity, ["intermediate"], ["output4"], name="node4", tags=["tag2"]),
            node(identity, ["intermediate"], ["output_5_output_5_1"], name="ns_5_node_5_1"),
            node(identity, ["intermediate"], ["output_6_output_6_1"], name="ns_6_node_6_1"),
            node(identity, ["intermediate"], ["output_7_output_7_1"], name="ns_7_node_7_1"),
        ],
        tags="pipeline0",
    )
    return {
        "__default__": pipeline,
        "ds": pipeline,
        "namespaced.pipeline": pipeline,
    }
"""
