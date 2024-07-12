import json
import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

from kedro.framework.startup import ProjectMetadata

_bundle_config_template = """
# This is a Databricks asset bundle definition for dab.
# See https://docs.databricks.com/dev-tools/bundles/index.html for documentation.
bundle:
  name: {{ .project_slug }}

artifacts:
  default:
    type: whl
    build: kedro package
    path: .

include:
  - resources/*.yml
  - resources/**/*.yml
  - resources/*.yaml
  - resources/**/*.yaml

targets:
  # The 'dev' target, used for development purposes.
  # Whenever a developer deploys using 'dev', they get their own copy.
  dev:
    # We use 'mode: development' to make sure everything deployed to this target gets a prefix
    # like '[dev my_user_name]'. Setting this mode also disables any schedules and
    # automatic triggers for jobs and enables the 'development' mode for Delta Live Tables pipelines.
    mode: development
    default: true
    workspace:
      host: {{workspace_host}}

  # The 'prod' target, used for production deployment.
  prod:
    # For production deployments, we only have a single copy, so we override the
    # workspace.root_path default of
    # /Users/${workspace.current_user.userName}/.bundle/${bundle.target}/${bundle.name}
    # to a path that is not specific to the current user.
    #
    # By making use of 'mode: production' we enable strict checks
    # to make sure we have correctly configured this target.
    mode: production
    workspace:
      host: {{workspace_host}}
      root_path: /Shared/.bundle/prod/${bundle.name}
    {{- if not is_service_principal}}
    run_as:
      # This runs as {{user_name}} in production. Alternatively,
      # a service principal could be used here using service_principal_name
      # (see Databricks documentation).
      user_name: {{user_name}}
    {{end -}}
"""

_bundle_init_template = {
    "welcome_message": "Creating a Databricks asset bundle definition...",
    "min_databricks_cli_version": "v0.212.2",
    "properties": {
        "project_name": {
            "order": 1,
            "type": "string",
            "default": "kedro project",
            "pattern": "^[^.\\\\/A-Z]{3,}$",
            "pattern_match_failure_message": 'Project name must be at least 3 characters long and cannot contain the following characters: "\\", "/", " ", ".", and must be all lowercase letters.',
            "description": "\nProject Name. Default",
        },
        "project_slug": {
            "order": 2,
            "type": "string",
            "default": "{{ ((regexp `[- ]`).ReplaceAllString .project_name `_`) -}}",
            "description": "\nProject slug. Default",
            "hidden": True,
        },
    },
    "success_message": "\n*** Asset Bundle successfully created for '{{.project_name}}'! ***",
}

_bundle_override_template = """
# Files named `databricks*` or `databricks/**` will be used to apply overrides to the
# generated asset bundle resources. The overrides should be specified according to the
# Databricks REST API's `Create a new job` endpoint. To learn more, visit their
# documentation at https://docs.databricks.com/api/workspace/jobs/create

{default_key}:
    job_clusters:
        - job_cluster_key: {default_key}
          new_cluster:
              spark_version: 14.3.x-scala2.12
              node_type_id: Standard_DS4_v2
              num_workers: 1
              spark_env_vars:
                  KEDRO_LOGGING_CONFIG: f"/dbfs/FileStore/{package_name}/conf/logging.yml"
    tasks:
        - task_key: {default_key}
          job_cluster_key: {default_key}
"""

_databricks_run_template = """
import argparse
import logging

from kedro.framework.project import configure_project
from kedro.framework.session import KedroSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", dest="env", type=str)
    parser.add_argument("--conf-source", dest="conf_source", type=str)
    parser.add_argument("--package-name", dest="package_name", type=str)
    parser.add_argument("--nodes", dest="nodes", type=str)

    args = parser.parse_args()
    env = args.env
    conf_source = args.conf_source
    package_name = args.package_name
    nodes = args.nodes

    # https://kb.databricks.com/notebooks/cmd-c-on-object-id-p0.html
    logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
    logging.getLogger("py4j.py4j.clientserver").setLevel(logging.ERROR)

    configure_project(package_name)
    with KedroSession.create(env=env, conf_source=conf_source) as session:
        session.run(node_names=[node.strip() for node in nodes.split(",")])


if __name__ == "__main__":
    main()
"""


def write_bundle_template(metadata: ProjectMetadata):
    log = logging.getLogger(metadata.package_name)
    log.info("Creating Databricks asset bundle configuration...")
    if shutil.which("databricks") is None:  # pragma: no cover
        raise Exception("databricks CLI is not installed")

    config = {
        "project_name": metadata.package_name,
        "project_slug": metadata.package_name,
    }

    assets_dir = tempfile.mkdtemp()
    assets_dir = Path(assets_dir)
    with open(assets_dir / "databricks_template_schema.json", "w") as f:
        f.write(json.dumps(_bundle_init_template))

    template_dir = assets_dir / "template"
    template_dir.mkdir(exist_ok=True)
    with open(f"{template_dir}/databricks.yml.tmpl", "w") as f:
        f.write(_bundle_config_template)

    template_params = tempfile.NamedTemporaryFile(delete=False)
    template_params.write(json.dumps(config).encode())
    template_params.close()

    config_path = metadata.project_path / "databricks.yml"
    if config_path.exists():
        raise FileExistsError(
            f"{config_path} already exists. To reinitialize, delete the file and try again."
        )

    # We utilize the databricks CLI to create the bundle configuration.
    # This is a bit hacky, but it allows the plugin to tap into the authentication
    # mechanism of the databricks CLI and thereby avoid the need to store credentials
    # in the plugin.
    result = subprocess.run(
        [
            "databricks",
            "bundle",
            "init",
            assets_dir.as_posix(),
            "--config-file",
            template_params.name,
            "--output-dir",
            metadata.project_path.as_posix(),
        ],
        stdout=subprocess.PIPE,
        check=False,
    )

    if result.returncode != 0:
        raise Exception(
            f"Failed to create Databricks asset bundle configuration: {result.stdout}"
        )

    shutil.rmtree(assets_dir)


def write_override_template(metadata: ProjectMetadata, default_key: str):
    log = logging.getLogger(metadata.package_name)
    log.info("Creating Databricks asset bundle override configuration...")
    p = Path(metadata.project_path) / "conf" / "base" / "databricks.yml"
    if not p.exists():
        with open(p, "w") as f:
            f.write(
                _bundle_override_template.format(
                    default_key=default_key, package_name="package_name"
                )
            )


def write_databricks_run_script(metadata: ProjectMetadata):
    log = logging.getLogger(metadata.package_name)
    log.info("Creating Databricks run script...")
    p = (
        Path(metadata.project_path)
        / "src"
        / metadata.package_name
        / "databricks_run.py"
    )

    with open(p, "w") as f:
        f.write(_databricks_run_template)
