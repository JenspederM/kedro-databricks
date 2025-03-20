from kedro.pipeline import Pipeline, node

from kedro_databricks.constants import DEFAULT_TARGET
from kedro_databricks.deploy import DeployController
from kedro_databricks.plugin import commands
from tests.utils import reset_init


def identity(arg):
    return arg


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
    ],
    tags="pipeline0",
)

pipelines = {
    "__default__": pipeline,
    "ds": pipeline,
}


def test_deploy(cli_runner, metadata, custom_username):
    """Test the `deploy` command"""
    reset_init(metadata)
    deploy_fail = ["databricks", "deploy"]
    result = cli_runner.invoke(commands, deploy_fail, obj=metadata)
    assert result.exit_code == 1, (result.exit_code, result.stdout)

    init_cmd = ["databricks", "init", "--provider", "azure"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    override_path = metadata.project_path / "conf" / DEFAULT_TARGET / "databricks.yml"
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert metadata.project_path.exists(), "Project path not created"
    assert metadata.project_path.is_dir(), "Project path is not a directory"
    assert override_path.exists(), "Override file not created"

    deploy_cmd = ["databricks", "deploy", "--bundle"]
    result = cli_runner.invoke(commands, deploy_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)

    controller = DeployController(metadata)
    resources = controller.log_deployed_resources(
        pipelines, only_dev=True, _custom_username=custom_username
    )
    assert len(resources) > 0, f"There are no resources: {resources}"
    assert all(
        metadata.package_name in p.name for p in resources
    ), f"Package name not in resource: {[p.name for p in resources if metadata.package_name not in p.name]}"


def test_deploy_prod(cli_runner, metadata, custom_username):
    """Test the `deploy` command"""
    reset_init(metadata)
    deploy_fail = ["databricks", "deploy"]
    result = cli_runner.invoke(commands, deploy_fail, obj=metadata)
    assert result.exit_code == 1, (result.exit_code, result.stdout)

    init_cmd = ["databricks", "init", "--provider", "azure"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    override_path = metadata.project_path / "conf" / "prod" / "databricks.yml"
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert metadata.project_path.exists(), "Project path not created"
    assert metadata.project_path.is_dir(), "Project path is not a directory"
    assert override_path.exists(), "Override file not created"

    deploy_cmd = [
        "databricks",
        "deploy",
        "--env",
        "prod",
        "--bundle",
        "--",
        "--target",
        "prod",
    ]
    result = cli_runner.invoke(commands, deploy_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)

    controller = DeployController(metadata)
    resources = controller.log_deployed_resources(
        pipelines, _custom_username=custom_username
    )
    assert len(resources) > 0, f"There are no resources: {resources}"
    assert all(
        metadata.package_name in p.name for p in resources
    ), f"Package name not in resource: {[p.name for p in resources if metadata.package_name not in p.name]}"


def test_deploy_with_conf(cli_runner, metadata):
    """Test the `deploy` command"""
    reset_init(metadata)
    deploy_fail = ["databricks", "deploy"]
    result = cli_runner.invoke(commands, deploy_fail, obj=metadata)
    assert result.exit_code == 1, (result.exit_code, result.stdout)

    CONF_KEY = "custom_conf"

    init_cmd = ["databricks", "init", "--provider", "azure"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    override_path = metadata.project_path / CONF_KEY / DEFAULT_TARGET / "databricks.yml"
    override_path.parent.mkdir(parents=True, exist_ok=True)
    override_path.write_text(
        """
        default:
            job_clusters:
                - job_cluster_key: default
                new_cluster:
                    spark_version: 14.3.x-scala2.12
                    node_type_id: Standard_DS3_v2
                    num_workers: 1
                    spark_env_vars:
                        KEDRO_LOGGING_CONFIG: "/dbfs/FileStore/develop_eggs/conf/logging.yml"
            tasks:
                - task_key: default
                job_cluster_key: default
        """
    )
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert metadata.project_path.exists(), "Project path not created"
    assert metadata.project_path.is_dir(), "Project path is not a directory"

    settings = metadata.project_path / "src" / metadata.package_name / "settings.py"
    original_settings = settings.read_text()
    with open(settings, "a"):
        settings.write_text(f"CONF_SOURCE = '{CONF_KEY}'")

    deploy_cmd = [
        "databricks",
        "deploy",
        "--bundle",
        f"--conf-source={CONF_KEY}",
    ]
    result = cli_runner.invoke(commands, deploy_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)

    controller = DeployController(metadata)
    controller._untar_conf(CONF_KEY)
    conf_path = (
        metadata.project_path / "dist" / CONF_KEY / DEFAULT_TARGET / "databricks.yml"
    )
    files = list((metadata.project_path / "dist" / CONF_KEY).rglob("*"))
    assert conf_path.exists(), f"Conf file not created - found {files}"
    assert (
        conf_path.read_text() == override_path.read_text()
    ), f"Conf file not copied - found {files}"
    settings.write_text(original_settings)
