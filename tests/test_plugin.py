import yaml
from kedro_databricks.plugin import commands


def test_databricks_init(kedro_project, cli_runner, metadata):
    """Test the `init` command"""
    (kedro_project / "databricks.yml").unlink(missing_ok=True)
    (kedro_project / "conf" / "base" / "databricks.yml").unlink(missing_ok=True)
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

    command = ["databricks", "init"]
    result = cli_runner.invoke(commands, command, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)


def test_databricks_bundle(kedro_project, cli_runner, metadata):
    """Test the `bundle` command"""
    bundle_fail = ["databricks", "bundle", "--default", "_deault"]
    result = cli_runner.invoke(commands, bundle_fail, obj=metadata)
    assert result.exit_code == 1, (result.exit_code, result.stdout)

    init_cmd = ["databricks", "init"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)

    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert metadata.project_path.exists(), "Project path not created"
    assert metadata.project_path.is_dir(), "Project path is not a directory"

    override_path = metadata.project_path / "conf" / "base" / "databricks.yml"
    assert override_path.exists(), "Override file not created"

    command = ["databricks", "bundle"]
    result = cli_runner.invoke(commands, command, obj=metadata)

    resource_dir = kedro_project / "resources"

    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert resource_dir.exists(), "Resource directory not created"
    assert resource_dir.is_dir(), "Resource directory is not a directory"

    files = [p.name for p in resource_dir.rglob("*")]
    files.sort()

    assert files == [
        f"{metadata.package_name}.yml",
        f"{metadata.package_name}_ds.yml",
    ], f"Resource files not created: {', '.join(files)}"

    resources = []
    for p in files:
        with open(resource_dir / p) as f:
            resource = yaml.safe_load(f)
            resources.append(resource)

    for resource, file_name in zip(resources, files):
        assert resource.get("resources") is not None

        jobs = resource["resources"]["jobs"]
        assert len(jobs) == 1

        job = jobs.get(file_name.split(".")[0])
        assert job is not None

        tasks = job.get("tasks")
        assert tasks is not None
        assert len(tasks) == 5

        for i, task in enumerate(tasks):
            assert task.get("task_key") == f"node{i}"
            assert task.get("job_cluster_key") == "default"


def test_deploy(kedro_project, cli_runner, metadata):
    """Test the `deploy` command"""
    deploy_fail = ["databricks", "deploy"]
    result = cli_runner.invoke(commands, deploy_fail, obj=metadata)
    assert result.exit_code == 1, (result.exit_code, result.stdout)


#     init_cmd = ["databricks", "init"]
#     result = cli_runner.invoke(commands, init_cmd, obj=metadata)
#     assert result.exit_code == 0, (result.exit_code, result.stdout)

#     bundle_cmd = ["databricks", "bundle"]
#     result = cli_runner.invoke(commands, bundle_cmd, obj=metadata)
#     assert result.exit_code == 0, (result.exit_code, result.stdout)

#     deploy_cmd = ["databricks", "deploy"]
#     result = cli_runner.invoke(commands, deploy_cmd, obj=metadata)
#     assert result.exit_code == 0, (result.exit_code, result.stdout)
