import yaml

from kedro_databricks.constants import DEFAULT_TARGET
from kedro_databricks.plugin import commands
from kedro_databricks.utils.create_target_configs import (
    _get_targets,
    _read_databricks_config,
)


def test_databricks_bundle_fail(cli_runner, metadata):
    bundle_fail = ["databricks", "bundle", "--default-key", "_deault"]
    result = cli_runner.invoke(commands, bundle_fail, obj=metadata)
    assert result.exit_code == 1, (result.exit_code, result.stdout)


def test_databricks_bundle_with_overrides(kedro_project, cli_runner, metadata):
    init_cmd = ["databricks", "init", "--provider", "azure"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert metadata.project_path.exists(), "Project path not created"
    assert metadata.project_path.is_dir(), "Project path is not a directory"
    assert metadata.project_path / "databricks.yml", "Databricks config not created"

    databricks_config = _read_databricks_config(metadata.project_path)
    targets = _get_targets(databricks_config)
    for target in targets:
        override_path = metadata.project_path / "conf" / target / "databricks.yml"
        assert (
            override_path.exists()
        ), f"Resource Overrides at {override_path} does not exist"

    command = ["databricks", "bundle", "--env", DEFAULT_TARGET]
    result = cli_runner.invoke(commands, command, obj=metadata)
    resource_dir = kedro_project / "resources"
    conf_dir = kedro_project / "conf" / DEFAULT_TARGET
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

    init_cmd = ["databricks", "init", "--provider", "azure"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    override_path = (
        metadata.project_path
        / "conf"
        / "sub_pipeline"
        / DEFAULT_TARGET
        / "databricks.yml"
    )
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    override_path.unlink(missing_ok=True)
    assert not override_path.exists(), "Override file not created"

    command = [
        "databricks",
        "bundle",
        "--env",
        DEFAULT_TARGET,
        "--conf-source",
        "conf/sub_pipeline",
    ]
    result = cli_runner.invoke(commands, command, obj=metadata)
    resource_dir = kedro_project / "resources"
    conf_dir = kedro_project / "conf" / "sub_pipeline" / DEFAULT_TARGET
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

    init_cmd = ["databricks", "init", "--provider", "azure"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert metadata.project_path.exists(), "Project path not created"
    assert metadata.project_path.is_dir(), "Project path is not a directory"
    assert metadata.project_path / "databricks.yml", "Databricks config not created"

    databricks_config = _read_databricks_config(metadata.project_path)
    targets = _get_targets(databricks_config)
    for target in targets:
        override_path = metadata.project_path / "conf" / target / "databricks.yml"
        assert (
            override_path.exists()
        ), f"Resource Overrides at {override_path} does not exist"

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


def test_databricks_bundle_with_params(kedro_project, cli_runner, metadata):
    """Test the `bundle` command"""
    init_cmd = ["databricks", "init", "--provider", "aws"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert metadata.project_path.exists(), "Project path not created"
    assert metadata.project_path.is_dir(), "Project path is not a directory"
    assert metadata.project_path / "databricks.yml", "Databricks config not created"

    databricks_config = _read_databricks_config(metadata.project_path)
    targets = _get_targets(databricks_config)
    for target in targets:
        override_path = metadata.project_path / "conf" / target / "databricks.yml"
        assert (
            override_path.exists()
        ), f"Resource Overrides at {override_path} does not exist"

    command = [
        "databricks",
        "bundle",
        "--env",
        "dev",
        "--params",
        "run_date={{job.parameters.run_date}},run_id={{job.parameters.run_id}}",
        "--overwrite",
    ]
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
            params = task.get("python_wheel_task").get("parameters")
            assert "--params" in params
            for j, param in enumerate(params):
                if param == "--params":
                    assert (
                        params[j + 1]
                        == "run_date={{job.parameters.run_date}},run_id={{job.parameters.run_id}}"
                    )
                    break
