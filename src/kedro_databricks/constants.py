from importlib import metadata, resources

from packaging.version import Version

from kedro_databricks.config import config

TEMPLATES = resources.files("kedro_databricks").joinpath("templates")

KEDRO_VERSION = Version(metadata.version("kedro"))
"""Kedro version used to build this plugin."""

MINIMUM_DATABRICKS_VERSION = [0, 205, 0]
"""Minimum Databricks version required for this plugin."""

MAX_TASK_KEY_LENGTH = 100
"""Maximum number of characters in a task key in Databricks jobs."""

JOB_KEY_ORDER = [
    "name",
    "description",
    "parameters",
    "environments",
    "job_clusters",
    "tasks",
    "access_control_list",
    "budget_policy_id",
    "continuous",
    "deployment",
    "edit_mode",
    "email_notifications",
    "format",
    "git_source",
    "health",
    "max_concurrent_runs",
    "notification_settings",
    "performance_target",
    "queue",
    "run_as",
    "schedule",
    "tags",
    "timeout_seconds",
    "trigger",
    "webhook_notifications",
]
"""Order of keys in the job configuration for Databricks jobs."""

TASK_KEY_ORDER = [
    "task_key",
    "description",
    "depends_on",
    "environment_key",
    "job_cluster_key",
    "existing_cluster_id",
    "libraries",
    "new_cluster",
    "compute",
    "disable_auto_optimization",
    "health",
    "run_if",
    "max_retries",
    "min_retry_interval_millis",
    "retry_on_timeout",
    "timeout_seconds",
    "notification_settings",
    "email_notifications",
    "webhook_notifications",
    "alert_task",
    "clean_rooms_notebook_task",
    "condition_task",
    "dashboard_task",
    "dbt_task",
    "for_each_task",
    "notebook_task",
    "pipeline_task",
    "power_bi_task",
    "python_wheel_task",
    "run_job_task",
    "spark_jar_task",
    "spark_python_task",
    "spark_submit_task",
    "sql_task",
]
"""Order of keys in the task configuration for Databricks jobs."""

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
conf/{config.default_env}/**
!conf/{config.default_env}/.gitkeep
""".strip()
"""Content to be added to `.gitignore` for Kedro Databricks configurations."""
