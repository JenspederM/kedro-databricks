from importlib import resources
from pathlib import Path

from kedro import __version__ as kedro_version

PACKAGE_ROOT = Path(__file__).parent.parent.parent
EXAMPLE_ROOT = PACKAGE_ROOT / "examples"


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
TEMPLATES = resources.files("kedro_databricks").joinpath("templates")

DEFAULT_TARGET = "dev"
DEFAULT_CONF_FOLDER = "conf"
DEFAULT_CONFIG_KEY = "default"
DEFAULT_CONFIG_HELP = "Set the key for the default configuration"
DEFAULT_PROVIDER = "azure"
NODE_TYPE_MAP = {
    "aws": "m5.xlarge",
    "azure": "Standard_DS3_v2",
    "gcp": "n1-standard-4",
}
CONF_HELP = "Set the conf folder. Default to `conf`."
PROVIDER_PROMPT = """
Please select your cloud provider:
- azure
- aws
- gcp
"""
OVERRIDE_KEY_MAP = {
    "job_clusters": "job_cluster_key",
    "tasks": "task_key",
}
INVALID_CONFIG_MSG = """
No `databricks.yml` file found. Maybe you forgot to initialize the Databricks bundle?

You can initialize the Databricks bundle by running:

```
kedro databricks init
```
"""
