from importlib import metadata, resources

from packaging.version import Version

TEMPLATES = resources.files("kedro_databricks").joinpath("templates")

KEDRO_VERSION = Version(metadata.version("kedro"))
"""Kedro version used to build this plugin."""

MINIMUM_DATABRICKS_VERSION = [0, 205, 0]
"""Minimum Databricks version required for this plugin."""

MAX_TASK_KEY_LENGTH = 100
"""Maximum number of characters in a task key in Databricks jobs."""

TASK_KEY_ORDER = [
    "task_key",
    "job_cluster_key",
    "new_cluster",
    "depends_on",
    "spark_python_task",
    "python_wheel_task",
]
"""Order of keys in the task configuration for Databricks jobs."""

JOB_KEY_ORDER = [
    "name",
    "tags",
    "access_control_list",
    "email_notifications",
    "schedule",
    "max_concurrent_runs",
    "job_clusters",
    "tasks",
]
"""Order of keys in the job configuration for Databricks jobs."""

DEFAULT_CATALOG = "workspace"
"""Default catalog for Databricks targets."""

DEFAULT_CATALOG_HELP = "Set the catalog for Databricks targets"
"""Help text for the default catalog option."""

DEFAULT_SCHEMA = "default"
"""Default schema for Databricks targets."""

DEFAULT_SCHEMA_HELP = "Set the schema for Databricks targets"
"""Help text for the default schema option."""

DEFAULT_ENV = "dev"
"""Default target environment for Databricks configurations."""

DEFAULT_CONF_FOLDER = "conf"
"""Default folder for Kedro configurations."""

DEFAULT_CONFIG_KEY = "default"
"""Default configuration key for Databricks jobs."""

DEFAULT_CONFIG_KEY_HELP = "Set the key for the default configuration"
"""Help text for the default configuration key option."""

DEFAULT_CONFIG_GENERATOR = "node"
"""Default resource generator for Databricks Asset Bundle."""

DEFAULT_CONFIG_GENERATOR_HELP = "Generator used to create resources. Options are 'node' (create a job for each node) or 'pipeline' (create a single job for the entire pipeline)."
"""Help text for the resource generator option."""


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
conf/{DEFAULT_ENV}/**
!conf/{DEFAULT_ENV}/.gitkeep
""".strip()
"""Content to be added to `.gitignore` for Kedro Databricks configurations."""
