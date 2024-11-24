import yaml

from kedro_databricks.plugin import commands


def test_databricks_bundle_fail(cli_runner, metadata):
    bundle_fail = ["databricks", "bundle", "--default", "_deault"]
    result = cli_runner.invoke(commands, bundle_fail, obj=metadata)
    assert result.exit_code == 1, (result.exit_code, result.stdout)


def test_databricks_bundle_with_overrides(kedro_project, cli_runner, metadata):
    init_cmd = ["databricks", "init"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    override_path = metadata.project_path / "conf" / "base" / "databricks.yml"
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert override_path.exists(), "Override file not created"

    command = ["databricks", "bundle", "--env", "dev"]
    result = cli_runner.invoke(commands, command, obj=metadata)
    resource_dir = kedro_project / "resources"
    conf_dir = kedro_project / "conf" / "dev"
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert resource_dir.exists(), "Resource directory not created"
    assert resource_dir.is_dir(), "Resource directory is not a directory"
    assert conf_dir.exists(), "Configuration directory not created"

    files = [p.name for p in resource_dir.rglob("*")]
    files.sort()
    assert files == [
        f"{metadata.package_name}.yml",
        f"{metadata.package_name}_ds.yml",
    ], f"Resource files not created: {', '.join(files)}"

    resources_with_overrides = []
    for p in files:
        with open(resource_dir / p) as f:
            resource = yaml.safe_load(f)
            resources_with_overrides.append(resource)

    for resource, file_name in zip(resources_with_overrides, files):
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
            params = task.get("python_wheel_task").get("parameters")
            for j, param in enumerate(params):
                if param == "--env":
                    assert params[j + 1] == "dev"


def test_databricks_bundle_with_conf(kedro_project, cli_runner, metadata):
    """Test the `bundle` command"""

    init_cmd = ["databricks", "init"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    override_path = (
        metadata.project_path / "conf" / "sub_pipeline" / "base" / "databricks.yml"
    )
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    override_path.unlink(missing_ok=True)
    assert not override_path.exists(), "Override file not created"

    command = ["databricks", "bundle", "--env", "dev", "--conf", "conf/sub_pipeline"]
    result = cli_runner.invoke(commands, command, obj=metadata)
    resource_dir = kedro_project / "resources"
    conf_dir = kedro_project / "conf" / "sub_pipeline" / "dev"
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert resource_dir.exists(), "Resource directory not created"
    assert resource_dir.is_dir(), "Resource directory is not a directory"
    assert conf_dir.exists(), "Configuration directory not created"

    files = [p.name for p in resource_dir.rglob("*")]
    files.sort()
    assert files == [
        f"{metadata.package_name}.yml",
        f"{metadata.package_name}_ds.yml",
    ], f"Resource files not created: {', '.join(files)}"

    resources_without_overrides = []
    for p in files:
        with open(resource_dir / p) as f:
            resource = yaml.safe_load(f)
            resources_without_overrides.append(resource)

    for resource, file_name in zip(resources_without_overrides, files):
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
            params = task.get("python_wheel_task").get("parameters")
            for j, param in enumerate(params):
                if param == "--env":
                    assert params[j + 1] == "dev"


def test_databricks_bundle_without_overrides(kedro_project, cli_runner, metadata):
    """Test the `bundle` command"""

    init_cmd = ["databricks", "init"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    override_path = metadata.project_path / "conf" / "base" / "databricks.yml"
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    override_path.unlink(missing_ok=True)
    assert not override_path.exists(), "Override file not created"

    command = ["databricks", "bundle", "--env", "dev"]
    result = cli_runner.invoke(commands, command, obj=metadata)
    resource_dir = kedro_project / "resources"
    conf_dir = kedro_project / "conf" / "dev"
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert resource_dir.exists(), "Resource directory not created"
    assert resource_dir.is_dir(), "Resource directory is not a directory"
    assert conf_dir.exists(), "Configuration directory not created"

    files = [p.name for p in resource_dir.rglob("*")]
    files.sort()
    assert files == [
        f"{metadata.package_name}.yml",
        f"{metadata.package_name}_ds.yml",
    ], f"Resource files not created: {', '.join(files)}"

    resources_without_overrides = []
    for p in files:
        with open(resource_dir / p) as f:
            resource = yaml.safe_load(f)
            resources_without_overrides.append(resource)

    for resource, file_name in zip(resources_without_overrides, files):
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
            params = task.get("python_wheel_task").get("parameters")
            for j, param in enumerate(params):
                if param == "--env":
                    assert params[j + 1] == "dev"
