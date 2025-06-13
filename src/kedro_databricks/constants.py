from importlib import resources

from kedro import __version__ as kedro_version

TEMPLATES = resources.files("kedro_databricks").joinpath("templates")

KEDRO_VERSION = [int(x) for x in kedro_version.split(".")]
"""Kedro version used to build this plugin."""

MINIMUM_DATABRICKS_VERSION = [0, 205, 0]
"""Minimum Databricks version required for this plugin."""

TASK_KEY_ORDER = [
    "task_key",
    "job_cluster_key",
    "new_cluster",
    "depends_on",
    "spark_python_task",
    "python_wheel_task",
]
"""Order of keys in the task configuration for Databricks jobs."""

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
"""Order of keys in the workflow configuration for Databricks jobs."""

DEFAULT_TARGET = "dev"
"""Default target environment for Databricks configurations."""

DEFAULT_CONF_FOLDER = "conf"
"""Default folder for Kedro configurations."""

DEFAULT_CONFIG_KEY = "default"
"""Default configuration key for Databricks jobs."""

DEFAULT_CONFIG_HELP = "Set the key for the default configuration"
"""Help text for the default configuration key option."""

DEFAULT_PROVIDER = "azure"
"""Default cloud provider for Databricks Asset Bundle."""

OVERRIDE_KEY_MAP = {
    "job_clusters": "job_cluster_key",
    "tasks": "task_key",
    "depends_on": "task_key",
    "environments": "environment_key",
    "on_start": "id",
    "on_success": "id",
    "on_failure": "id",
    "on_duration_warning_threshold_exceeded": "id",
    "on_streaming_backlog_exceeded": "id",
    "rules": "metric",
    "parameters": "name",
    "libraries": "whl",
    "subscriptions": "user_name",
}
"""Map of keys that should be overridden in the Databricks configuration."""

IGNORED_OVERRIDE_KEYS = ["init_scripts", "access_control_list"]
"""Keys that should be ignored when overriding the Databricks configuration."""

NODE_TYPE_MAP = {
    "aws": "m5.xlarge",
    "azure": "Standard_DS3_v2",
    "gcp": "n1-standard-4",
}
"""Map of node types for different cloud providers."""

PROVIDER_PROMPT = """
Please select your cloud provider:
- azure
- aws
- gcp
"""
"""Prompt for selecting the cloud provider for Databricks Asset Bundle."""

INVALID_CONFIG_MSG = """
No `databricks.yml` file found. Maybe you forgot to initialize the Databricks bundle?

You can initialize the Databricks bundle by running:

```
kedro databricks init
```
"""
"""Message displayed when no `databricks.yml` file is found in the project."""

GITIGNORE = f"""
# Kedro Databricks
.databricks
conf/{DEFAULT_TARGET}/**
!conf/{DEFAULT_TARGET}/.gitkeep
""".strip()
"""Content to be added to `.gitignore` for Kedro Databricks configurations."""
