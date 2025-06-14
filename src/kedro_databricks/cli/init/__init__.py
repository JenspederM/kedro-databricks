import json
import shutil
import tempfile
from pathlib import Path

import tomlkit
from kedro.framework.startup import ProjectMetadata

from kedro_databricks.cli.init.create_target_configs import create_target_configs
from kedro_databricks.constants import GITIGNORE, NODE_TYPE_MAP, TEMPLATES
from kedro_databricks.logger import get_logger
from kedro_databricks.utils import (
    Command,
    assert_databricks_cli,
    require_databricks_run_script,
)

log = get_logger("init")


def init(
    metadata: ProjectMetadata, provider: str, default_key: str, *databricks_args: str
):
    """Initialize a Databricks Asset Bundle.

    This function creates a Databricks Asset Bundle in the current project
    directory. It also creates a Databricks configuration file and a
    Databricks target configuration file.

    Args:
        metadata (ProjectMetadata): The project metadata.
        provider (str): The provider to use. Valid providers are "azure", "aws", or "gcp".
        default_key (str): The default key to use for the Databricks target.
        *databricks_args: Additional arguments to be passed to the `databricks` CLI.

    Raises:
        RuntimeError: If the `databricks` CLI is not installed or the wrong version is used.
        ValueError: If the provider is not valid.
    """
    assert_databricks_cli()
    log.info("Initializing Databricks Asset Bundle...")
    config_path, node_type_id = _validate_inputs(metadata, provider)
    _databricks_init(metadata, *databricks_args)
    log.info(f"Created {config_path.relative_to(metadata.project_path)}")
    create_target_configs(metadata, node_type_id=node_type_id, default_key=default_key)
    _update_gitignore(metadata)
    if require_databricks_run_script():  # pragma: no cover - Might be removed in future
        log.warning(
            "Kedro version less than 0.19.8 requires a script to run tasks on Databricks. "
        )
        _write_databricks_run_script(metadata)
    log.info(
        f"Successfully initialized Databricks Asset Bundle in {metadata.project_path}"
    )


def _validate_inputs(metadata: ProjectMetadata, provider: str):
    if provider not in NODE_TYPE_MAP:
        raise ValueError(
            f"Invalid provider '{provider}'. Valid providers are: {', '.join(NODE_TYPE_MAP.keys())}"
        )
    config_path = metadata.project_path / "databricks.yml"
    if config_path.exists():
        raise RuntimeError(
            f"{config_path.relative_to(metadata.project_path)} already exists."
        )
    return config_path, NODE_TYPE_MAP[provider]


def _databricks_init(metadata: ProjectMetadata, *databricks_args):
    assets_dir, template_params = _prepare_template(metadata)
    init_cmd = [
        "databricks",
        "bundle",
        "init",
        assets_dir.as_posix(),
        "--config-file",
        template_params.as_posix(),
        "--output-dir",
        metadata.project_path.as_posix(),
    ] + list(databricks_args)
    result = Command(init_cmd, log=log, warn=True).run()
    if result.returncode != 0:  # pragma: no cover
        err = "\n".join(result.stdout)
        raise RuntimeError(f"Failed to initialize Databricks Asset Bundle\n{err}")
    shutil.rmtree(assets_dir)


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
