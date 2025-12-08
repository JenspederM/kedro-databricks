from typing import Any

from kedro_databricks.constants import (
    DEFAULT_CONFIG_KEY,
    IGNORED_OVERRIDE_KEYS,
    OVERRIDE_KEY_MAP,
)
from kedro_databricks.utilities.common import (
    get_defaults,
    get_lookup_key,
    get_old_value,
    update_list_by_key,
)
from kedro_databricks.utilities.logger import get_logger
from kedro_databricks.utilities.resource_overrider.abstract_resource_overrider import (
    AbstractResourceOverrider,
)

log = get_logger("JobsResourceOverrider")


class JobsResourceOverrider(AbstractResourceOverrider):
    """Override a Databricks jobs resource with the default key."""

    def override(
        self,
        resource_key: str,
        resource: dict[str, Any],
        overrides: dict[str, Any],
        default_key: str = DEFAULT_CONFIG_KEY,
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
        job_overrides, task_overrides = self._get_job_overrides(
            resource_key=resource_key,
            default_key=default_key,
            overrides=overrides,
        )
        return self._override_job(
            job=resource,
            job_overrides=job_overrides,
            task_overrides=task_overrides,
            default_key=default_key,
        )

    def _override_job(
        self,
        job: dict,
        job_overrides: dict,
        task_overrides: dict = {},
        default_key: str = "default",
    ):
        result = {**job}

        for key, value in job_overrides.items():
            old_value = get_old_value(result, key, value)
            if isinstance(value, dict) and isinstance(old_value, dict):
                result[key] = self._override_job(
                    job=old_value,
                    job_overrides=value,
                    task_overrides={},
                    default_key=default_key,
                )
            elif isinstance(value, list) and isinstance(old_value, list):
                if isinstance(value[0], dict) and key not in IGNORED_OVERRIDE_KEYS:
                    lookup_key = get_lookup_key(key, OVERRIDE_KEY_MAP)
                    if lookup_key:
                        result[key] = update_list_by_key(
                            old=old_value,
                            new=value,
                            lookup_key=lookup_key,
                            callback=self._override_job,
                            default=task_overrides if key == "tasks" else {},
                            default_key=default_key,
                        )
                elif key == "parameters":
                    # Special case for parameters, which can be a list of strings
                    log.debug(f"Overriding parameters for key {key} with value {value}")
                    result[key] = old_value + value
                else:
                    result[key] = value
            else:
                result[key] = job_overrides.get(key, old_value)
        return result

    def _get_job_overrides(
        self, resource_key: str, default_key: str, overrides: dict[str, Any]
    ):
        default_overrides = overrides.pop(default_key, {})
        job_overrides = overrides.pop(resource_key, {})
        all_overrides = {**default_overrides, **job_overrides}
        default_task = get_defaults(
            all_overrides.get("tasks", []), "task_key", default_key
        )
        return all_overrides, default_task
