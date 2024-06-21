import logging

from typing import Any

from kedro.framework.project import PACKAGE_NAME
from kedro.pipeline import Pipeline

from kedro_databricks.utils import WORKFLOW_KEY_ORDER, _sort_dict
from kedro_databricks.utils import (
    TASK_KEY_ORDER,
    WORKFLOW_KEY_ORDER,
    _remove_nulls_from_dict,
    _sort_dict,
)

DEFAULT = "default"

log = logging.getLogger(__name__)


def _create_task(name, depends_on, job_cluster_id):
    """Create a Databricks task for a given node.

    Args:
        name (str): name of the node
        depends_on (List[Node]): list of nodes that the task depends on
        job_cluster_id (str): ID of the job cluster to run the task on

    Returns:
        Dict[str, Any]: a Databricks task
    """
    ## Follows the Databricks REST API schema. See "tasks" in the link below
    ## https://docs.databricks.com/api/workspace/jobs/create

    task = {
        "task_key": name,
        "job_cluster_key": job_cluster_id,
        "libraries": [{"whl": "../dist/*.whl"}],
        "depends_on": [{"task_key": dep.name} for dep in depends_on],
        "python_wheel_task": {
            "package_name": PACKAGE_NAME,
            "entry_point": "databricks_run",
            "parameters": [
                "--nodes",
                name,
                "--conf-source",
                f"/dbfs/FileStore/{PACKAGE_NAME}/conf",
                "--package-name",
                PACKAGE_NAME,
            ],
        },
    }

    return _sort_dict(task, TASK_KEY_ORDER)


def _create_workflow(name: str, pipeline: Pipeline):
    """Create a Databricks workflow for a given pipeline.

    Args:
        name (str): name of the pipeline
        pipeline (Pipeline): Kedro pipeline object

    Returns:
        Dict[str, Any]: a Databricks workflow
    """
    ## Follows the Databricks REST API schema
    ## https://docs.databricks.com/api/workspace/jobs/create
    workflow = {
        "name": name,
        "tasks": [
            _create_task(node.name, depends_on=deps, job_cluster_id="default")
            for node, deps in pipeline.node_dependencies.items()
        ],
        "format": "MULTI_TASK",
    }

    return _remove_nulls_from_dict(_sort_dict(workflow, WORKFLOW_KEY_ORDER))


def _validate(workflow: dict[str, Any]) -> tuple[bool, str]:
    job_clusters = workflow.get("job_clusters", [])
    errors = []
    for cluster in job_clusters:
        if cluster.get("new_cluster") is not None:
            spark_env = cluster["new_cluster"].get("spark_env_vars", {})
            if "KEDRO_LOGGING_CONFIG" not in spark_env:
                errors.append(
                    f"KEDRO_LOGGING_CONFIG not found in spark_env_vars for cluster \"{cluster.get('job_cluster_key')}\""
                )

    return errors


def apply_resource_overrides(
    resources: dict, config: dict, default_key: str = DEFAULT, package_name=PACKAGE_NAME
):
    conf_default = config.get(default_key, {})
    task_default = conf_default.get("tasks", [])

    errors = _validate(conf_default)
    if len(errors) > 0:
        raise ValueError(f"Default configuration is not valid: {', '.join(errors)}")

    if len(task_default) > 0:
        if len(task_default) > 1:
            raise ValueError(f"Only one {default_key} task configuration is allowed.")
        task_default = task_default[0]
        if task_default.get("task_key") != default_key:
            raise ValueError(f"task_key cannot be set in {default_key}.")
        if task_default.get("depends_on") is not None:
            raise ValueError(f"depends_on cannot be set in {default_key}.")

    for name, resource in resources.items():
        log.debug(f"Applying overrides for pipeline '{name}'.")
        wf = resource["resources"]["jobs"][name]
        errors = _validate(wf)

        if len(errors) > 0:
            raise ValueError(f"Workflow {name} is not valid: {', '.join(errors)}")

        wf_conf = config.get(name, conf_default)
        if wf_conf.get("name") is not None:
            raise ValueError("name cannot be set in the pipeline configuration.")

        task_conf = wf_conf.pop("tasks", None)
        if task_conf is not None:
            task_conf = {task.pop("task_key"): task for task in task_conf}
            task_default = task_conf.get(default_key, task_default)

            if any(v.get("task_key") is not None for v in task_conf.values()):
                raise ValueError(f"task_key cannot be overwritten.")
            if any(v.get("depends_on") is not None for v in task_conf.values()):
                raise ValueError(f"depends_on cannot be overwritten.")

            wf_tasks = wf.get("tasks", [])
            for task in wf_tasks:
                task_conf = task_conf.get(task["task_key"], task_default)
                task.update(task_conf)
            wf["tasks"] = wf_tasks

        spark_env_vars = (
            wf_conf.get("job_clusters", [{}])[0]
            .get("new_cluster", {})
            .get("spark_env_vars", {})
        )

        if "KEDRO_LOGGING_CONFIG" not in spark_env_vars:
            spark_env_vars["KEDRO_LOGGING_CONFIG"] = (
                f"/dbfs/FileStore/{package_name}/conf/logging.yml"
            )

        wf.update(wf_conf)
        resource["resources"]["jobs"][name] = _sort_dict(wf, WORKFLOW_KEY_ORDER)

    return resources


def generate_resources(
    pipelines: dict[str, Pipeline], package_name=PACKAGE_NAME
) -> dict[str, dict[str, Any]]:
    """Generate Databricks resources for the given pipelines.

    Finds all pipelines in the project and generates Databricks asset bundle resources
    for each according to the Databricks REST API.

    Args:
        pipelines (dict[str, Pipeline]): A dictionary of pipeline names and their Kedro pipelines

    Returns:
        dict[str, dict[str, Any]]: A dictionary of pipeline names and their Databricks resources
    """

    jobs = {}

    for name, pipeline in pipelines.items():
        if len(pipeline.nodes) > 0:
            wf_name = (
                f"{package_name}_{name}" if name != "__default__" else package_name
            )
            wf = _create_workflow(wf_name, pipeline)
            log.debug(f"Workflow '{wf_name}' created successfully.")
            log.debug(wf)
            jobs[wf_name] = wf

    resources = {name: {"resources": {"jobs": {name: wf}}} for name, wf in jobs.items()}
    log.info("Databricks resources generated successfully.")
    log.debug(resources)
    return resources
