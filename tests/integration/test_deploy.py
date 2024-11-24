from kedro_databricks.plugin import commands
from tests.utils import reset_init


def test_deploy(cli_runner, metadata):
    """Test the `deploy` command"""
    reset_init(metadata)
    deploy_fail = ["databricks", "deploy"]
    result = cli_runner.invoke(commands, deploy_fail, obj=metadata)
    assert result.exit_code == 1, (result.exit_code, result.stdout)

    init_cmd = ["databricks", "init"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    override_path = metadata.project_path / "conf" / "base" / "databricks.yml"
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert metadata.project_path.exists(), "Project path not created"
    assert metadata.project_path.is_dir(), "Project path is not a directory"
    assert override_path.exists(), "Override file not created"

    deploy_cmd = ["databricks", "deploy", "--bundle", "--debug"]
    result = cli_runner.invoke(commands, deploy_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)


def test_deploy_with_conf(cli_runner, metadata):
    """Test the `deploy` command"""
    reset_init(metadata)
    deploy_fail = ["databricks", "deploy"]
    result = cli_runner.invoke(commands, deploy_fail, obj=metadata)
    assert result.exit_code == 1, (result.exit_code, result.stdout)

    init_cmd = ["databricks", "init"]
    result = cli_runner.invoke(commands, init_cmd, obj=metadata)
    override_path = (
        metadata.project_path / "conf" / "sub_pipeline" / "base" / "databricks.yml"
    )
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

    deploy_cmd = [
        "databricks",
        "deploy",
        "--bundle",
        "--debug",
        "--conf=conf/sub_pipeline",
    ]
    result = cli_runner.invoke(commands, deploy_cmd, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)
