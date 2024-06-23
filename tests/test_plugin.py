import pytest
from kedro_databricks.plugin import commands

import logging
import yaml

log = logging.getLogger(__name__)


def test_databricks_init(kedro_project, cli_runner, metadata):
    """Test the `init` command"""
    command = ["databricks", "init"]
    result = cli_runner.invoke(commands, command, obj=metadata)

    files = [f"{f.parent.name}/{f.name}" for f in kedro_project.rglob("*")]
    assert len(files) > 0, "Found no files in the directory."

    config_path = kedro_project / "databricks.yml"
    override_path = kedro_project / "conf" / "base" / "databricks.yml"
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert config_path.exists(), f"Configuration at {config_path} does not exist"
    assert (
        override_path.exists()
    ), f"Resource Overrides at {override_path} does not exist"


def test_databricks_bundle(kedro_project, cli_runner, metadata):
    """Test the `bundle` command"""
    command = ["databricks", "bundle"]
    result = cli_runner.invoke(commands, command, obj=metadata)

    resource_dir = kedro_project / "resources"

    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert resource_dir.exists(), "Resource directory not created"
    assert resource_dir.is_dir(), "Resource directory is not a directory"

    files = [p.name for p in resource_dir.rglob("*")]

    assert files == [
        f"{metadata.package_name}.yml",
        f"{metadata.package_name}_ds.yml",
    ], (
        files,
        "Resource files not created",
    )

    docs = []
    for p in files:
        with open(resource_dir / p) as f:
            doc = yaml.safe_load(f)
            docs.append(doc)

    for doc, fname in zip(docs, files):
        assert doc.get("resources") is not None

        jobs = doc["resources"]["jobs"]
        assert len(jobs) == 1

        job = jobs.get(fname.split(".")[0])
        assert job is not None

        tasks = job.get("tasks")
        assert tasks is not None
        assert len(tasks) == 5

        for i, task in enumerate(tasks):
            assert task.get("task_key") == f"node{i}"

    result = cli_runner.invoke(commands, command, obj=metadata)
    assert result.exit_code == 1, (result.exit_code, result.stdout)
