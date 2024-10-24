from __future__ import annotations

import json
import logging
import re
import shutil
import tempfile
from importlib import resources
from pathlib import Path

import tomlkit
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.utils import has_databricks_cli, run_cmd

NODE_TYPE_MAP = {
    "aws": "m5.xlarge",
    "azure": "Standard_DS3_v2",
    "gcp": "n1-standard-4",
}

DEFAULT_PROVIDER = "azure"
DEFAULT_NODE_TYPE_ID = NODE_TYPE_MAP.get(DEFAULT_PROVIDER, None)
TEMPLATES = resources.files("kedro_databricks").joinpath("templates")

assert NODE_TYPE_MAP is not None, "Invalid default provider"


class InitController:
    def __init__(self, metadata: ProjectMetadata) -> None:
        self.metadata = metadata
        self.project_path = metadata.project_path
        self.package_name = metadata.package_name
        self.log = logging.getLogger(self.package_name)

    def bundle_init(self):
        MSG = "Creating databricks configuration"
        if not has_databricks_cli():  # pragma: no cover
            raise Exception("databricks CLI is not installed")

        config_path = self.project_path / "databricks.yml"
        if config_path.exists():
            self.log.warning(
                f"{MSG}: {config_path.relative_to(self.project_path)} already exists."
            )
            return

        assets_dir = Path(tempfile.mkdtemp())
        shutil.copy(
            TEMPLATES / "databricks_template_schema.json",
            assets_dir / "databricks_template_schema.json",
        )

        template_dir = assets_dir / "template"
        template_dir.mkdir(exist_ok=True)
        shutil.copy(
            TEMPLATES / "databricks.yml.tmpl", template_dir / "databricks.yml.tmpl"
        )

        config = {"project_name": self.package_name, "project_slug": self.package_name}
        template_params = tempfile.NamedTemporaryFile(delete=False)
        template_params.write(json.dumps(config).encode())
        template_params.close()

        # We utilize the databricks CLI to create the bundle configuration.
        # This is a bit hacky, but it allows the plugin to tap into the authentication
        # mechanism of the databricks CLI and thereby avoid the need to store credentials
        # in the plugin.
        run_cmd(
            [
                "databricks",
                "bundle",
                "init",
                assets_dir.as_posix(),
                "--config-file",
                template_params.name,
                "--output-dir",
                self.project_path.as_posix(),
            ],
            msg=MSG,
        )
        self.log.info(f"{MSG}: Wrote {config_path.relative_to(self.project_path)}")

        shutil.rmtree(assets_dir)

    def write_kedro_databricks_config(self, default_key: str, provider_name: str):
        MSG = "Creating bundle override configuration"
        override_path = Path(self.project_path) / "conf" / "base" / "databricks.yml"
        node_type_id = NODE_TYPE_MAP.get(provider_name, DEFAULT_NODE_TYPE_ID)
        if override_path.exists():
            self.log.warning(
                f"{MSG}: {override_path.relative_to(self.project_path)} already exists."
            )
            return

        with open(override_path, "w") as f:
            template = TEMPLATES / "kedro_databricks_config.yml.tmpl"
            f.write(
                template.read_text().format(
                    default_key=default_key,
                    package_name=self.package_name,
                    node_type_id=node_type_id,
                )
            )
        self.log.info(f"{MSG}: Wrote {override_path.relative_to(self.project_path)}")

    def write_databricks_run_script(self):
        MSG = "Creating Databricks run script"
        script_path = (
            self.project_path / "src" / self.package_name / "databricks_run.py"
        )
        toml_path = self.project_path / "pyproject.toml"
        shutil.copy(TEMPLATES / "databricks_run.py", script_path)
        self.log.info(f"{MSG}: Wrote {script_path.relative_to(self.project_path)}")

        with open(toml_path) as f:
            toml = tomlkit.load(f)

        scripts = toml.get("project", {}).get("scripts", {})
        if "databricks_run" not in scripts:
            scripts["databricks_run"] = f"{self.package_name}.databricks_run:main"
            toml["project"]["scripts"] = scripts

        self.log.info(
            f"{MSG}: Added script to {toml_path.relative_to(self.project_path)}"
        )
        with open(toml_path, "w") as f:
            tomlkit.dump(toml, f)

    def substitute_catalog_paths(self):
        MSG = "Substituting DBFS paths"
        conf_dir = self.metadata.project_path / "conf"
        envs = [d for d in conf_dir.iterdir() if d.is_dir()]
        regex = r"(.*/dbfs/FileStore/)(.*)(/data.*)"
        for env in envs:
            path = conf_dir / env / "catalog.yml"
            self.log.info(f"{MSG}: Checking {path.relative_to(self.project_path)}")

            if not path.exists():
                self.log.warning(
                    f"{MSG}: {path.relative_to(self.project_path)} does not exist."
                )
                continue

            with open(path) as f:
                content = f.readlines()

            new_content = self._parse_content(regex, path, content)

            with open(path, "w") as f:
                f.writelines(new_content)

    def _parse_content(self, regex, path, content):
        new_content = []
        for line in content:
            new_line = re.sub(regex, f"\\g<1>{self.package_name}\\g<3>", line)
            if new_line != line:
                self.log.info(
                    f"{path.relative_to(self.project_path)}: "
                    f"Substituted: {line.strip()} -> {new_line.strip()}"
                )
            new_content.append(new_line)
        return new_content
