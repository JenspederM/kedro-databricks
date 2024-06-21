from pathlib import Path
import subprocess
import tempfile
import json
import shutil

import yaml

_template = """
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

_databricks_template = {
    "welcome_message": "Welcome to the Databricks Kedro Bundle. For detailed information on project generation, see the README at https://github.com/jenspederm/databricks-kedro-bundle.",
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
    "success_message": "\n*** Kedro Asset Bundle created in '{{.project_name}}' directory! ***\n\nPlease refer to the README.md for further instructions on getting started.",
}


def create_databricks_config(path: str, package_name: str):
    if shutil.which("databricks") is None:
        raise Exception("databricks CLI is not installed")

    config = {"project_name": package_name, "project_slug": package_name}

    assets_dir = tempfile.mkdtemp()
    assets_dir = Path(assets_dir)
    with open(assets_dir / "databricks_template_schema.json", "w") as f:
        f.write(json.dumps(_databricks_template))

    template_dir = assets_dir / "template"
    template_dir.mkdir(exist_ok=True)
    with open(f"{template_dir}/databricks.yml.tmpl", "w") as f:
        f.write(_template)

    template_params = tempfile.NamedTemporaryFile(delete=False)
    template_params.write(json.dumps(config).encode())
    template_params.close()

    # We utilize the databricks CLI to create the bundle configuration.
    # This is a bit hacky, but it allows the plugin to tap into the authentication
    # mechanism of the databricks CLI and thereby avoid the need to store credentials
    # in the plugin.
    subprocess.call(
        [
            "databricks",
            "bundle",
            "init",
            assets_dir.as_posix(),
            "--config-file",
            template_params.name,
            "--output-dir",
            path,
        ]
    )

    shutil.rmtree(assets_dir)


def write_default_config(path: str, default_key: str, package_name: str):
    with open(path, "w") as f:
        try:
            conf = yaml.safe_load(f)
            if conf is None:
                conf = {}
        except:
            conf = {}

        conf[default_key] = {
            "job_clusters": [
                {
                    "job_cluster_key": default_key,
                    "new_cluster": {
                        "spark_version": "14.3.x-scala2.12",
                        "node_type_id": "Standard_D4ds_v4",
                        "num_workers": 1,
                        "spark_env_vars": {
                            "KEDRO_LOGGING_CONFIG": f"/dbfs/FileStore/{package_name}/conf/logging.yml",
                        },
                    },
                }
            ],
            "tasks": [{"task_key": default_key, "job_cluster_key": default_key}],
        }

        yaml.dump(conf, f, default_flow_style=False, indent=4, sort_keys=False)
