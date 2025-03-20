from __future__ import annotations

import json
import logging
import shutil
import tempfile
from pathlib import Path

import tomlkit
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.constants import GITIGNORE, TEMPLATES
from kedro_databricks.utils.common import Command
from kedro_databricks.utils.create_target_configs import create_target_configs
from kedro_databricks.utils.has_databricks import has_databricks_cli


class InitController:
    def __init__(self, metadata: ProjectMetadata) -> None:
        self.metadata: ProjectMetadata = metadata
        self.project_path: Path = metadata.project_path
        self.package_name: str = metadata.package_name
        self.log: logging.Logger = logging.getLogger(self.package_name)

    def bundle_init(self, databricks_args: list[str]):
        """Initialize Databricks Asset Bundle configuration.

        Args:
            databricks_args: Additional arguments to pass to the databricks CLI.

        Raises:
            Exception: If the databricks CLI is not installed.

        Returns:
            subprocess.CompletedProcess: The result of the databricks CLI command.
        """
        MSG = "Creating databricks configuration"
        if not has_databricks_cli():  # pragma: no cover - this is a system dependency
            raise Exception("databricks CLI is not installed")

        config_path = self.project_path / "databricks.yml"
        if config_path.exists():
            self.log.warning(
                f"{MSG}: {config_path.relative_to(self.project_path)} already exists."
            )
            return

        assets_dir = Path(tempfile.mkdtemp())
        shutil.copy(
            str(TEMPLATES / "databricks_template_schema.json"),
            str(assets_dir / "databricks_template_schema.json"),
        )

        template_dir = assets_dir / "template"
        template_dir.mkdir(exist_ok=True)
        shutil.copy(
            str(TEMPLATES / "databricks.yml.tmpl"),
            str(template_dir / "databricks.yml.tmpl"),
        )

        config = {"project_name": self.package_name, "project_slug": self.package_name}
        template_params = tempfile.NamedTemporaryFile(delete=False)
        template_params.write(json.dumps(config).encode())
        template_params.close()

        # We utilize the databricks CLI to create the bundle configuration.
        # This is a bit hacky, but it allows the plugin to tap into the authentication
        # mechanism of the databricks CLI and thereby avoid the need to store credentials
        # in the plugin.
        init_cmd = [
            "databricks",
            "bundle",
            "init",
            assets_dir.as_posix(),
            "--config-file",
            template_params.name,
            "--output-dir",
            self.project_path.as_posix(),
        ] + databricks_args
        result = Command(init_cmd, msg=MSG, warn=True).run()
        self.log.info(f"{MSG}: Wrote {config_path.relative_to(self.project_path)}")
        shutil.rmtree(assets_dir)
        return result

    def create_override_configs(
        self, node_type_id: str, default_key: str
    ):  # pragma: no cover - this is tested separately
        """Create override configurations for Databricks targets."

        Args:
            node_type_id: The node type ID for the Databricks provider.
            default_key: The default key for the Databricks target.

        Raises:
            FileNotFoundError: If the project path does not exist.
        """
        create_target_configs(
            self.metadata, node_type_id=node_type_id, default_key=default_key
        )

    def update_gitignore(self):
        gitignore_path = self.project_path / ".gitignore"
        if not gitignore_path.exists():
            gitignore_path.touch()

        current_gitignore = gitignore_path.read_text()

        with open(gitignore_path, "w") as f:
            f.write(f"{GITIGNORE}\n{current_gitignore}")

    def write_databricks_run_script(self):
        MSG = "Creating Databricks run script"
        script_path = (
            self.project_path / "src" / self.package_name / "databricks_run.py"
        )
        toml_path = self.project_path / "pyproject.toml"
        shutil.copy(str(TEMPLATES / "databricks_run.py"), str(script_path))
        self.log.info(f"{MSG}: Wrote {script_path.relative_to(self.project_path)}")

        with open(toml_path) as f:
            toml = tomlkit.load(f)

        scripts = toml.get("project", {}).get("scripts", {})
        if "databricks_run" not in scripts:
            scripts["databricks_run"] = f"{self.package_name}.databricks_run:main"
            toml["project"]["scripts"] = scripts  # type: ignore

        self.log.info(
            f"{MSG}: Added script to {toml_path.relative_to(self.project_path)}"
        )
        with open(toml_path, "w") as f:
            tomlkit.dump(toml, f)
