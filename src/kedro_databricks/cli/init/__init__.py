import json
import shutil
import tempfile
from pathlib import Path

import tomlkit
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.cli.deploy import DatabricksCli
from kedro_databricks.cli.init.create_target_configs import create_target_configs
from kedro_databricks.cli.init.inject_local_hook import transform_spark_hook
from kedro_databricks.core.constants import GITIGNORE, TEMPLATES
from kedro_databricks.core.logger import get_logger
from kedro_databricks.core.utils import require_databricks_run_script

log = get_logger("init")


def init(metadata: ProjectMetadata, default_key: str, *databricks_args: str):
    """Initialize a Databricks Asset Bundle.

    This function creates a Databricks Asset Bundle in the current project
    directory. It also creates a Databricks configuration file and a
    Databricks target configuration file.

    Args:
        metadata (ProjectMetadata): The project metadata.
        default_key (str): The default key to use for the Databricks target.
        *databricks_args: Additional arguments to be passed to the `databricks` CLI.

    Raises:
        RuntimeError: If the `databricks` CLI is not installed or the wrong version is used.
    """
    log.info("Initializing Databricks Asset Bundle...")
    dbcli = DatabricksCli(metadata, additional_args=list(databricks_args))
    assets_dir, template_params = _prepare_template(metadata)
    dbcli.init(assets_dir, template_params)
    log.info(f"Initialized Databricks Asset Bundle in {metadata.project_path}")
    create_target_configs(metadata, default_key=default_key)
    _update_gitignore(metadata)
    hooks_path = metadata.project_path / "src" / metadata.package_name / "hooks.py"
    if hooks_path.exists():
        transform_spark_hook(hooks_path.as_posix())
    if require_databricks_run_script():  # pragma: no cover - Might be removed in future
        log.warning(
            "Kedro version less than 0.19.8 requires a script to run tasks on Databricks. "
        )
        _write_databricks_run_script(metadata)
    log.info(
        f"Successfully initialized Databricks Asset Bundle in {metadata.project_path}"
    )


def _prepare_template(metadata: ProjectMetadata):
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
    config = {
        "project_name": metadata.package_name,
        "project_slug": metadata.package_name,
    }
    params_file = assets_dir / "params.json"
    params_file.touch()
    params_file.write_text(json.dumps(config))
    return assets_dir, params_file


def _update_gitignore(metadata: ProjectMetadata):
    gitignore_path = metadata.project_path / ".gitignore"
    if not gitignore_path.exists():
        gitignore_path.touch()
    current_gitignore = gitignore_path.read_text()
    with open(gitignore_path, "w") as f:
        f.write(f"{GITIGNORE}\n{current_gitignore}")


def _write_databricks_run_script(metadata: ProjectMetadata):
    script_path = (
        metadata.project_path / "src" / metadata.package_name / "databricks_run.py"
    )
    toml_path = metadata.project_path / "pyproject.toml"
    shutil.copy(str(TEMPLATES / "databricks_run.py"), str(script_path))
    log.info(f"Wrote {script_path.relative_to(metadata.project_path)}")

    with open(toml_path) as f:
        toml = tomlkit.load(f)

    scripts = toml.get("project", {}).get("scripts", {})
    if "databricks_run" not in scripts:
        scripts["databricks_run"] = f"{metadata.package_name}.databricks_run:main"
        toml["project"]["scripts"] = scripts  # type: ignore

    log.info(f"Added script to {toml_path.relative_to(metadata.project_path)}")
    with open(toml_path, "w") as f:
        tomlkit.dump(toml, f)
