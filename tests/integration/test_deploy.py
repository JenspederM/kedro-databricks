import shutil

from click.testing import Result
from kedro.pipeline import Pipeline, node

from kedro_databricks.cli.deploy.get_deployed_resources import get_deployed_resources
from kedro_databricks.constants import DEFAULT_TARGET
from kedro_databricks.plugin import commands
from tests.utils import identity, reset_init

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
        node(
            identity,
            ["intermediate"],
            ["outputs.output_1.output_1_1"],
            name="outputs.output_1.output_1_1",
        ),
        node(
            identity,
            ["intermediate"],
            ["outputs.output_1.output_1_2"],
            name="outputs.output_1.output_1_2",
        ),
        node(identity, ["intermediate"], ["outputs.output_2"], name="outputs.output_2"),
    ],
    tags="pipeline0",
)

pipelines = {
    "__default__": pipeline,
    "ds": pipeline,
    "namespaced.pipeline": pipeline,
}


def _validate_deploy(
    metadata,
    result: Result,
    custom_username: str | None = None,
):
    assert result.exit_code == 0 and "Deployment complete!\n" in result.stdout, (
        result.exit_code,
        result.stdout,
        result.exception,
    )
    resources = get_deployed_resources(
        metadata, pipelines, only_dev=True, _custom_username=custom_username
    )
    assert len(resources) > 0, f"There are no resources: {resources}"
    assert all(
        metadata.package_name in p.name for p in resources
    ), f"Package name not in resource: {[p.name for p in resources if metadata.package_name not in p.name]}"


def test_deploy_fail(cli_runner, metadata):
    """Test the `deploy` command"""
    reset_init(metadata)
    deploy_fail = ["databricks", "deploy"]
    result = cli_runner.invoke(commands, deploy_fail, obj=metadata)
    assert result.exit_code == 1, (result.exit_code, result.stdout, result.exception)


def test_bundled_deploy(
    kedro_project_with_init_destroy, custom_username, custom_provider
):
    """Test the `deploy` command"""
    # Arrange
    metadata, cli_runner = kedro_project_with_init_destroy

    # Act
    deploy_cmd = ["databricks", "deploy", "--bundle"]
    result = cli_runner.invoke(commands, deploy_cmd, obj=metadata)

    # Assert
    _validate_deploy(metadata=metadata, result=result, custom_username=custom_username)


def test_deploy(
    kedro_project_with_init_bundle_destroy, custom_username, custom_provider
):
    """Test the `deploy` command"""
    # Arrange
    metadata, cli_runner = kedro_project_with_init_bundle_destroy

    # Act
    deploy_cmd = ["databricks", "deploy"]
    result = cli_runner.invoke(commands, deploy_cmd, obj=metadata)

    # Assert
    _validate_deploy(metadata=metadata, result=result, custom_username=custom_username)


def test_deploy_prod(kedro_project_with_init_bundle_destroy, custom_username):
    """Test the `deploy` command"""
    # Arrange
    metadata, cli_runner = kedro_project_with_init_bundle_destroy

    # Act
    deploy_cmd = ["databricks", "deploy", "--env", "prod"]
    result = cli_runner.invoke(commands, deploy_cmd, obj=metadata)

    # Assert
    _validate_deploy(metadata=metadata, result=result, custom_username=custom_username)


def test_deploy_with_conf(kedro_project_with_init_bundle_destroy, custom_username):
    """Test the `deploy` command"""
    # Arrange
    metadata, cli_runner = kedro_project_with_init_bundle_destroy
    CONF_KEY = "custom_conf"

    # Act
    default_path = metadata.project_path / "conf" / "dev" / "databricks.yml"
    override_path = metadata.project_path / CONF_KEY / DEFAULT_TARGET / "databricks.yml"
    override_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(default_path, override_path)
    settings = metadata.project_path / "src" / metadata.package_name / "settings.py"
    original_settings = settings.read_text()
    with open(settings, "a") as f:
        f.write(f"\nCONF_SOURCE = '{CONF_KEY}'")
    deploy_cmd = ["databricks", "deploy", "--bundle", f"--conf-source={CONF_KEY}"]
    result = cli_runner.invoke(commands, deploy_cmd, obj=metadata)

    # Assert
    _validate_deploy(metadata=metadata, result=result, custom_username=custom_username)

    # Revert
    settings.write_text(original_settings)
