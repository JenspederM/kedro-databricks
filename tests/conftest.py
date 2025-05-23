"""
This file contains the fixtures that are reusable by any tests within
this directory. You don't need to import the fixtures as pytest will
discover them automatically. More info here:
https://docs.pytest.org/en/latest/fixture.html
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from click.testing import CliRunner
from dotenv import load_dotenv
from kedro.framework.cli.starters import create_cli as kedro_cli
from kedro.framework.startup import bootstrap_project
from pytest import fixture

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


@fixture(scope="session")
def custom_username():
    custom_username = os.getenv("CUSTOM_USERNAME")
    return custom_username


@fixture(name="cli_runner", scope="session")
def cli_runner():
    runner = CliRunner()
    with runner.isolated_filesystem():
        yield runner


def _create_kedro_settings_py(file_name: Path, patterns: list[str]):
    patterns_str = ", ".join([f'"{p}"' for p in patterns])
    content = f"""CONFIG_LOADER_ARGS = {{
    "base_env": "base",
    "default_run_env": "local",
    "config_patterns": {{
        "databricks": [{patterns_str}],  # configure the pattern for configuration files
    }}
}}
"""
    file_name.write_text(content)


@fixture(scope="session")
def kedro_project(cli_runner):
    cli_runner.invoke(
        # Supply name, tools, and example to skip interactive prompts
        kedro_cli,
        [
            "new",
            "-v",
            "--name",
            "Fake Project",
            "--tools",
            "none",
            "--example",
            "no",
        ],
    )
    pipeline_registry_py = """
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
        ],
        tags="pipeline0",
    )
    return {
        "__default__": pipeline,
        "ds": pipeline,
    }
    """

    project_path = Path().cwd() / "fake-project"
    (project_path / "src" / "fake_project" / "pipeline_registry.py").write_text(
        pipeline_registry_py
    )

    settings_file = project_path / "src" / "fake_project" / "settings.py"
    _create_kedro_settings_py(settings_file, ["databricks*", "databricks/**"])

    os.chdir(project_path)
    return project_path


@fixture(scope="session")
def metadata(kedro_project):
    # cwd() depends on ^ the isolated filesystem, created by CliRunner()
    project_path = kedro_project.resolve()
    metadata = bootstrap_project(project_path)
    return metadata
