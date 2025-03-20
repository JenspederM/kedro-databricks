from importlib import resources

from kedro import __version__ as kedro_version

TEMPLATES = resources.files("kedro_databricks").joinpath("templates")

KEDRO_VERSION = [int(x) for x in kedro_version.split(".")]
TASK_KEY_ORDER = [
    "task_key",
    "job_cluster_key",
    "new_cluster",
    "depends_on",
    "spark_python_task",
    "python_wheel_task",
]
WORKFLOW_KEY_ORDER = [
    "name",
    "tags",
    "access_control_list",
    "email_notifications",
    "schedule",
    "max_concurrent_runs",
    "job_clusters",
    "tasks",
]


DEFAULT_TARGET = "dev"
DEFAULT_CONF_FOLDER = "conf"
DEFAULT_CONFIG_KEY = "default"
DEFAULT_CONFIG_HELP = "Set the key for the default configuration"
DEFAULT_PROVIDER = "azure"

OVERRIDE_KEY_MAP = {
    "job_clusters": "job_cluster_key",
    "tasks": "task_key",
    "environments": "environment_key",
    "on_start": "id",
    "on_success": "id",
    "on_failure": "id",
    "on_duration_warning_threshold_exceeded": "id",
    "on_duration_failure_threshold_exceeded": "id",
    "rules": "metric",
    "parameters": "name",
    "libraries": "whl",
}
NODE_TYPE_MAP = {
    "aws": "m5.xlarge",
    "azure": "Standard_DS3_v2",
    "gcp": "n1-standard-4",
}

PROVIDER_PROMPT = """
Please select your cloud provider:
- azure
- aws
- gcp
"""
INVALID_CONFIG_MSG = """
No `databricks.yml` file found. Maybe you forgot to initialize the Databricks bundle?

You can initialize the Databricks bundle by running:

```
kedro databricks init
```
"""
GITIGNORE = f"""
# Kedro Databricks
.databricks
conf/{DEFAULT_TARGET}/**
!conf/{DEFAULT_TARGET}/.gitkeep
""".strip()
