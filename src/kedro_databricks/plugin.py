from __future__ import annotations

from pathlib import Path

import click

from kedro_databricks.utilities.plugin import Plugin


@click.group(name="Kedro-Databricks")
def commands():
    """Entry point for Kedro-Databricks commands"""
    pass


@commands.group(
    cls=Plugin,
    commands_dir=Path(__file__).parent / "commands",
    name="databricks",
)
def databricks_commands():
    """Databricks Asset Bundle commands

    These commands are used to manage Databricks Asset Bundles in a Kedro project.
    They allow you to initialize, bundle, deploy, run, and destroy Databricks asset bundles.
    """
    pass
