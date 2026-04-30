from abc import ABC, abstractmethod
from typing import Any

from kedro_databricks.config import config


class AbstractResourceOverrider(ABC):
    """Abstract base class for resource overriders."""

    @abstractmethod
    def override(
        self,
        resource_key: str,
        resource: dict[str, Any],
        overrides: dict[str, Any],
        default_key: str = config.workflow_default_key,
    ) -> dict[str, Any]: ...
