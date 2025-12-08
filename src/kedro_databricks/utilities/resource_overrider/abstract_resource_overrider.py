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
