from typing import Any

from kedro_databricks.config import config
from kedro_databricks.utilities.common import sort_dict
from kedro_databricks.utilities.resource_overrider import AbstractResourceOverrider


class DefaultResourceOverrider(AbstractResourceOverrider):
    """A default resource overrider that performs no overrides."""

    def override(
        self,
        resource_key: str,
        resource: dict[str, Any],
        overrides: dict[str, Any],
        default_key: str = config.workflow_default_key,
    ) -> dict[str, Any]:
        """Return the overrides unchanged.

        Args:
            resource_key: The key identifying the resource.
            default_key: The default key for overrides.
            resource: The original resource dictionary.
            overrides: The overrides to apply.

        Returns:
            dict[str, Any]: resource with overrides applied (unchanged).
        """
        if not isinstance(resource, dict):
            raise ValueError(f"resource must be a dictionary not {type(resource)}")
        if not isinstance(overrides, dict):
            raise ValueError(f"overrides must be a dictionary not {type(overrides)}")
        specific_overrides = overrides.pop(resource_key, {})
        default_overrides = overrides.pop(default_key, {})
        all_overrides = {**default_overrides, **specific_overrides}
        return sort_dict({**resource, **all_overrides})
