import ast
import json
import re
import shutil
import tempfile
from functools import reduce
from operator import add
from pathlib import Path

import click
import tomlkit
import yaml
from kedro.framework.startup import ProjectMetadata
from tomlkit.items import Table

import kedro_databricks.commands._options as option
from kedro_databricks.config import config
from kedro_databricks.constants import TEMPLATES
from kedro_databricks.utilities.common import (
    get_value_from_dotpath,
    require_databricks_run_script,
)
from kedro_databricks.utilities.databricks_cli import DatabricksCli
from kedro_databricks.utilities.logger import get_logger

log = get_logger("init")


@click.command()
@option.catalog
@option.schema
@option.default_key
@option.overwrite
@option.databricks_args
@click.pass_obj
def command(
    metadata: ProjectMetadata,
    catalog: str,
    schema: str,
    default_key: str,
    overwrite: bool,
    databricks_args: tuple[str, ...],
):
    """Initialize a Kedro project for Databricks Asset Bundles."""
    log.info("Initializing Databricks Asset Bundle...")
    dbcli = DatabricksCli(metadata, additional_args=list(databricks_args))
    assets_dir, template_params = _prepare_template(metadata)
    config_path = metadata.project_path / "databricks.yml"
    if config_path.exists() and not overwrite:
        raise FileExistsError(f"`databricks.yml` already exist at {config_path}")
    elif config_path.exists() and overwrite:
        config_path.unlink(missing_ok=True)
    validated_conf = dbcli.init(assets_dir, template_params)
    log.info(f"Initialized Databricks Asset Bundle in {metadata.project_path}")
    _create_target_configs(
        metadata,
        default_key=default_key,
        default_catalog=catalog,
        default_schema=schema,
        validated_conf=validated_conf,
    )
    _update_pyproject(metadata)
    _update_gitignore(metadata)
    hooks_path = metadata.project_path / "src" / metadata.package_name / "hooks.py"
    if hooks_path.exists():
        _transform_spark_hook(hooks_path.as_posix())
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


def _update_pyproject(metadata: ProjectMetadata):
    pyproject_path = metadata.project_path / "pyproject.toml"
    with open(pyproject_path) as f:
        pyproject = tomlkit.load(f)
    tool_table = pyproject.get("tool")
    if not tool_table:
        tool_table = tomlkit.table()
    elif not isinstance(tool_table, Table):
        raise TypeError(f"Unexpected tool table type: {type(tool_table)}")
    tool_table.update({"kedro-databricks": config.model_dump()})
    pyproject.update({"tool": tool_table})
    with open(pyproject_path, "w") as f:
        tomlkit.dump(pyproject, f)


def _update_gitignore(metadata: ProjectMetadata):
    gitignore_path = metadata.project_path / ".gitignore"
    if not gitignore_path.exists():
        gitignore_path.touch()
    current_gitignore = gitignore_path.read_text()
    lines_to_add = []
    ignore_lines = [
        ".databricks",
        f"conf/{config.default_env}/**!conf/{config.default_env}/.gitkeep",
    ]
    for ignore_line in ignore_lines:
        found = False
        for line in current_gitignore.splitlines():
            if ignore_line in line:
                found = True
                break
        if not found:
            lines_to_add.append(ignore_line)
    if lines_to_add:
        GITIGNORE = "\n".join(["# added by `kedro-databricks`"] + lines_to_add)
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


def _create_target_configs(
    metadata: ProjectMetadata,
    default_key: str,
    default_catalog: str,
    default_schema: str,
    validated_conf: dict,
):
    conf_dir = metadata.project_path / "conf"
    databricks_config = _read_databricks_config(metadata.project_path)
    bundle_name = _get_bundle_name(databricks_config)
    targets = _get_targets(databricks_config)
    for target_name in targets.keys():
        target_conf_dir = conf_dir / target_name
        target_conf_dir.mkdir(exist_ok=True)
        _save_gitkeep_file(target_conf_dir)
        target_config = _create_target_config(
            default_key=default_key,
            bundle_name=bundle_name,
            target_name=target_name,
        )
        _save_target_config(target_config, target_conf_dir)
        target_file_path = _make_target_file_path(
            catalog_name=default_catalog,
            schema_name=default_schema,
            volume_name=_create_volume_name(target_name, bundle_name, validated_conf),
            target_name=target_name
            if target_name != config.default_env
            else bundle_name,
        )
        _save_target_catalog(conf_dir, target_conf_dir, target_file_path)
        log.info(f"Created target config for {target_name} at {target_conf_dir}")


ENV_CHECK = ast.If(
    test=ast.Compare(
        left=ast.Attribute(
            value=ast.Name(id="context", ctx=ast.Load()),
            attr="env",
            ctx=ast.Load(),
        ),
        ops=[ast.NotEq()],
        comparators=[ast.Constant(value="local")],
    ),
    body=[ast.Return(value=None)],
    orelse=[],
)


class InjectEnvCheck(ast.NodeTransformer):
    """AST transformer to inject an environment check into the SparkHooks class."""

    def visit_ClassDef(self, node):
        """Visit class definitions to find SparkHooks and modify its methods.

        Args:
            node (ast.ClassDef): The class definition node.

        Returns:
            ast.ClassDef: The modified class definition node.
        """
        # Process class body (methods)
        if node.name != "SparkHooks":
            return node

        new_body = []
        for item in node.body:
            if (
                isinstance(item, ast.FunctionDef)
                and item.name == "after_context_created"
            ):
                # Insert check at beginning of function body
                # Determine insertion point:
                # If first node is a docstring expression, insert afterwards.
                insert_pos = 0
                if (
                    len(item.body) > 0
                    and isinstance(item.body[0], ast.Expr)
                    and isinstance(item.body[0].value, ast.Constant)
                    and isinstance(item.body[0].value.value, str)  # it is a docstring
                ):
                    insert_pos = 1

                # Don't double-insert if it's already there
                if len(item.body) > insert_pos and isinstance(
                    item.body[insert_pos], ast.If
                ):
                    return node
                item.body.insert(insert_pos, ENV_CHECK)
            new_body.append(item)

        node.body = new_body
        return node


def _transform_spark_hook(path: str):
    with open(path) as f:
        source = f.read()

    tree = ast.parse(source)
    tree = InjectEnvCheck().visit(tree)
    ast.fix_missing_locations(tree)
    new_source = ast.unparse(tree)

    with open(path, "w") as f:
        f.write(new_source)
    return new_source


def _create_volume_name(
    target_name: str, bundle_name: str, validated_conf: dict
) -> str:
    if target_name == config.default_env:
        short_name = get_value_from_dotpath(
            validated_conf, "workspace.current_user.short_name"
        )
        if not short_name:
            raise ValueError(
                "Could not determine the current user's short name from the configuration."
            )
        return short_name
    else:
        return bundle_name


def _create_target_config(
    default_key: str,
    bundle_name: str,
    target_name: str,
) -> dict:
    volume_name = (
        "\\${workspace.current_user.short_name}"
        if target_name == config.default_env
        else bundle_name
    )
    return {
        "resources": {
            "volumes": {
                f"{bundle_name}_volume": {
                    "catalog_name": "workspace",
                    "schema_name": "default",
                    "name": volume_name,
                    "comment": f"Created by kedro-databricks for target {target_name}",
                    "volume_type": "MANAGED",
                    "grants": [
                        {
                            "principal": "\\${workspace.current_user.userName}",
                            "privileges": ["READ_VOLUME", "WRITE_VOLUME"],
                        },
                    ],
                }
            },
            "jobs": {
                default_key: {
                    "environments": [
                        {
                            "environment_key": default_key,
                            "spec": {
                                "environment_version": "4",
                                "dependencies": ["../dist/*.whl"],
                            },
                        }
                    ],
                    "tasks": [
                        {
                            "task_key": default_key,
                            "environment_key": default_key,
                        }
                    ],
                }
            },
        }
    }


def _make_target_file_path(
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    target_name: str,
) -> str:
    return f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{target_name}"


def _substitute_file_path(string: str) -> str:
    """Substitute the file path in the catalog"""
    lines = string.splitlines(keepends=True)
    new_lines = []
    for line in lines:
        new_lines.append(
            re.sub(
                r"(.*:)\s*(?!https?://)(?:/[^/\s]+)*/?(data(?:/[^/\s]+)+).*",
                r"\g<1> ${_file_path}/\g<2>",
                line,
            )
        )
    sub = reduce(add, new_lines)
    return sub


def _save_target_catalog(
    conf_dir: Path, target_conf_dir: Path, target_file_path: str
):  # pragma: no cover
    with open(f"{conf_dir}/base/catalog.yml") as f:
        cat = f.read()
    target_catalog = _substitute_file_path(cat)
    with open(target_conf_dir / "catalog.yml", "w") as f:
        f.write("_file_path: " + target_file_path + "\n" + target_catalog)


def _save_target_config(target_config: dict, target_conf_dir: Path):  # pragma: no cover
    with open(target_conf_dir / "databricks.yml", "w") as f:
        yaml.dump(target_config, f)


def _save_gitkeep_file(target_conf_dir: Path):
    if not (target_conf_dir / ".gitkeep").exists():
        with open(target_conf_dir / ".gitkeep", "w") as f:
            f.write("")


def _read_databricks_config(project_path: Path) -> dict:
    with open(project_path / "databricks.yml") as f:
        conf = yaml.safe_load(f)
    return conf


def _get_bundle_name(config: dict) -> str:
    bundle_name = config.get("bundle", {}).get("name")
    if bundle_name is None:
        raise ValueError("No `bundle.name` found in databricks.yml")
    return bundle_name


def _get_targets(config: dict) -> dict:
    targets = config.get("targets")
    if targets is None:
        raise ValueError("No `targets` found in databricks.yml")
    return targets
