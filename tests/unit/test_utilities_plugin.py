from kedro_databricks.plugin import Path, commands


def test_plugin_list_commands(cli_runner, metadata):
    # Arrange
    commands_path = (
        Path(__file__).parent.parent.parent / "src" / "kedro_databricks" / "commands"
    )
    cmds = [
        p
        for p in commands_path.iterdir()
        if p.is_file() and p.suffix == ".py" and p.stem != "__init__"
    ]

    # Act
    result = cli_runner.invoke(
        commands,
        ["databricks", "--help"],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)

    for cmd in cmds:
        assert f"{cmd.stem}" in result.stdout


def test_plugin_get_command(cli_runner, metadata):
    # Act
    result = cli_runner.invoke(
        commands,
        ["databricks", "version"],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 0, (result.exit_code, result.stdout, result.exception)


def test_plugin_get_invalid_command(cli_runner, metadata):
    # Act
    result = cli_runner.invoke(
        commands,
        ["databricks", "non-existent-command"],
        obj=metadata,
    )

    # Assert
    assert result.exit_code == 2, (result.exit_code, result.stdout, result.exception)
