from typing import Any

from kedro_databricks.constants import DEFAULT_CONFIG_KEY
from kedro_databricks.utilities.common import sort_dict
from kedro_databricks.utilities.resource_overrider import AbstractResourceOverrider


class DefaultResourceOverrider(AbstractResourceOverrider):
    """A default resource overrider that performs no overrides."""

    def override(
        self,
        resource_key: str,
        resource: dict[str, Any],
        overrides: dict[str, Any],
        default_key: str = DEFAULT_CONFIG_KEY,
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
        default_overrides = overrides.pop(default_key, None)
        specific_overrides = overrides.pop(resource_key, None)
        all_overrides = {**(default_overrides or {}), **(specific_overrides or {})}
        return sort_dict({**resource, **all_overrides})
