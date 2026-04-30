from functools import reduce
from typing import Any

from fuso import merge, merge_list_of_dicts_by_key
from fuso.merge import create_merge_factory

from kedro_databricks.config import config
from kedro_databricks.constants import JOB_KEY_ORDER, TASK_KEY_ORDER
from kedro_databricks.resource_overrider.abstract_resource_overrider import (
    AbstractResourceOverrider,
)
from kedro_databricks.utilities.common import get_regex_values
from kedro_databricks.utilities.logger import get_logger

log = get_logger("JobsResourceOverrider")


def _notification_overrider(old, new):
    return create_merge_factory(
        merge_functions={
            "on_start": lambda old, new: merge_list_of_dicts_by_key(
                old or [], new or [], key="id"
            ),
            "on_success": lambda old, new: merge_list_of_dicts_by_key(
                old or [], new or [], key="id"
            ),
            "on_failure": lambda old, new: merge_list_of_dicts_by_key(
                old or [], new or [], key="id"
            ),
            "on_duration_warning_threshold_exceeded": lambda old, new: (
                merge_list_of_dicts_by_key(old or [], new or [], key="id")
            ),
            "on_streaming_backlog_exceeded": lambda old, new: (
                merge_list_of_dicts_by_key(old or [], new or [], key="id")
            ),
        },
        key_order=[],
    )(old or {}, new or {})


def _access_control_list_overrider(old: list[dict], new: list[dict]) -> list[dict]:
    old = old or []
    new = new or []
    old_groups = [o for o in old if o.get("group_name")]
    new_groups = [n for n in new if n.get("group_name")]
    old_users = [o for o in old if o.get("user_name")]
    new_users = [n for n in new if n.get("user_name")]
    old_spns = [o for o in old if o.get("service_principal_name")]
    new_spns = [n for n in new if n.get("service_principal_name")]
    merged_groups = merge_list_of_dicts_by_key(old_groups, new_groups, key="group_name")
    merged_users = merge_list_of_dicts_by_key(old_users, new_users, key="user_name")
    merged_spns = merge_list_of_dicts_by_key(
        old_spns, new_spns, key="service_principal_name"
    )
    return merged_groups + merged_users + merged_spns


def _libraries_overrider(old: list[dict], new: list[dict]) -> list[dict]:
    old = old or []
    new = new or []
    old_cran = [o for o in old if o.get("cran")]
    new_cran = [n for n in new if n.get("cran")]
    old_egg = [o for o in old if o.get("egg")]
    new_egg = [n for n in new if n.get("egg")]
    old_jar = [o for o in old if o.get("jar")]
    new_jar = [n for n in new if n.get("jar")]
    old_maven = [o for o in old if o.get("maven")]
    new_maven = [n for n in new if n.get("maven")]
    old_pypi = [o for o in old if o.get("pypi")]
    new_pypi = [n for n in new if n.get("pypi")]
    old_requirements = [o for o in old if o.get("requirements")]
    new_requirements = [n for n in new if n.get("requirements")]
    old_whl = [o for o in old if o.get("whl")]
    new_whl = [n for n in new if n.get("whl")]
    merged_cran = merge_list_of_dicts_by_key(old_cran, new_cran, key="package")
    merged_egg = merge_list_of_dicts_by_key(old_egg, new_egg, key="egg")
    merged_jar = merge_list_of_dicts_by_key(old_jar, new_jar, key="jar")
    merged_maven = merge_list_of_dicts_by_key(old_maven, new_maven, key="coordinates")
    merged_pypi = merge_list_of_dicts_by_key(old_pypi, new_pypi, key="package")
    merged_requirements = merge_list_of_dicts_by_key(
        old_requirements, new_requirements, key="requirements"
    )
    merged_whl = merge_list_of_dicts_by_key(old_whl, new_whl, key="whl")
    return (
        merged_cran
        + merged_egg
        + merged_jar
        + merged_maven
        + merged_pypi
        + merged_requirements
        + merged_whl
    )


_task_overrider = create_merge_factory(
    merge_functions={
        "depends_on": lambda old, new: merge_list_of_dicts_by_key(
            old or [], new or [], key="task_key"
        ),
        "webhook_notifications": _notification_overrider,
        "health": lambda old, new: merge(
            old,
            new,
            merge_functions={
                "rules": lambda o, n: merge_list_of_dicts_by_key(o, n, key="metric")
            },
        ),
        "libraries": _libraries_overrider,
    },
    key_order=TASK_KEY_ORDER,
)


def _tasks_overrider(old: list[dict], new: list[dict], default_key: str) -> list[dict]:
    old = old or []
    new = new or []
    tasks = {o.pop("task_key"): o for o in old if o.get("task_key")}
    overrides = {n.pop("task_key"): n for n in new if n.get("task_key")}
    default_overrides = overrides.pop(default_key, {})
    all_task_keys = set(list(tasks.keys()) + list(overrides.keys()))
    overriden_tasks = []
    for task_key in all_task_keys:
        if task_key == default_key or task_key.startswith("re:"):
            continue
        task = tasks.get(task_key, {})
        task_overrides = overrides.get(task_key, {})
        regex_overrides = get_regex_values(task_key, overrides)
        overriden_tasks.append(
            {
                "task_key": task_key,
                **reduce(
                    _task_overrider,
                    [
                        task,
                        default_overrides,
                        regex_overrides,
                        task_overrides,
                    ],
                ),
            }
        )
    return sorted(overriden_tasks, key=lambda x: x.get("task_key", ""))


class JobsResourceOverrider(AbstractResourceOverrider):
    """Override a Databricks jobs resource with the default key."""

    def override(
        self,
        resource_key: str,
        resource: dict[str, Any],
        overrides: dict[str, Any],
        default_key: str = config.workflow_default_key,
    ) -> dict[str, Any]:
        """Override the resources in a Databricks bundle.

        This function applies the given overrides to the resources in a Databricks bundle.

        Args:
            resource_key (str): the key identifying the resource
            resource (Dict): the Databricks jobs to override
            overrides (Dict): the overrides to apply
            default_key (str): the default key to use for overrides

        Raises:
            ValueError: if the job or overrides are not dictionaries
            ValueError: if the key in overrides is not found in OVERRIDE_KEY_MAP

        Returns:
            Dict[str, Any]: the Databricks bundle with the overrides applied
        """
        if not isinstance(resource, dict):
            raise ValueError(f"resource must be a dictionary not {type(resource)}")
        if not isinstance(overrides, dict):
            raise ValueError(f"overrides must be a dictionary not {type(overrides)}")
        overrider = create_merge_factory(
            merge_functions={
                "tasks": lambda old, new: _tasks_overrider(
                    old, new, default_key=default_key
                ),
                "environments": lambda old, new: merge_list_of_dicts_by_key(
                    old or [], new or [], key="environment_key"
                ),
                "job_clusters": lambda old, new: merge_list_of_dicts_by_key(
                    old or [], new or [], key="job_cluster_key"
                ),
                "access_control_list": _access_control_list_overrider,
                "webhook_notifications": _notification_overrider,
                "health": lambda old, new: merge(
                    old,
                    new,
                    merge_functions={
                        "rules": lambda o, n: merge_list_of_dicts_by_key(
                            o, n, key="metric"
                        )
                    },
                ),
                "parameters": lambda old, new: merge_list_of_dicts_by_key(
                    old, new, key="name"
                ),
            },
            key_order=JOB_KEY_ORDER,
        )
        default_overrides = overrides.pop(default_key, {})
        regex_overrides = get_regex_values(resource_key, overrides)
        resource_overrides = overrides.pop(resource_key, {})
        overriden = reduce(
            overrider,
            [
                resource,
                default_overrides,
                regex_overrides,
                resource_overrides,
            ],
        )
        return overriden
