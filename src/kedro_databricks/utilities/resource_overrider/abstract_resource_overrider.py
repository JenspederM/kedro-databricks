import re
from abc import ABC, abstractmethod
from typing import Any

from kedro_databricks.constants import DEFAULT_CONFIG_KEY


class AbstractResourceOverrider(ABC):
    """Abstract base class for resource overriders."""

    @abstractmethod
    def override(
        self,
        resource_key: str,
        resource: dict[str, Any],
        overrides: dict[str, Any],
        default_key: str = DEFAULT_CONFIG_KEY,
    ) -> dict[str, Any]: ...

    def get_regex_overrides(
        self,
        resource_key: str,
        overrides: dict[str, Any],
    ) -> dict[str, Any]:
        """Get the regex overrides for a given resource key.

        Args:
            resource_key (str): The key identifying the resource.
            overrides (dict[str, Any]): The overrides to apply.
        Returns:
            dict[str, Any]: The regex overrides for the given resource key.
        """
        regex_overrides = {}
        for key, value in overrides.items():
            if key.startswith("re:"):
                pattern = key[3:]
                if re.match(pattern, resource_key):
                    regex_overrides = value
        return regex_overrides
