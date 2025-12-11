import importlib.util
from pathlib import Path

import click


class Plugin(click.Group):
    """Kedro-Databricks plugin for Kedro CLI"""

    def __init__(self, commands_dir: Path, **kwargs):
        super().__init__(**kwargs)
        self.commands_dir = commands_dir

    def list_commands(self, ctx):
        cmds = []
        for cmd_file in self.commands_dir.glob("*.py"):
            if cmd_file.stem != "__init__":
                cmds.append(cmd_file.stem)
        cmds.sort()
        return cmds

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        cmd_path = self.commands_dir / f"{cmd_name}.py"
        if not cmd_path.exists():
            return None
        spec = importlib.util.spec_from_file_location(cmd_name, cmd_path)
        if (
            not spec or not spec.loader
        ):  # pragma: no cover - we already check for file existence and python extension
            raise ImportError(f"Cannot find spec for module {cmd_name}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.command
