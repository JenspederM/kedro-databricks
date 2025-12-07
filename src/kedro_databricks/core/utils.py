from __future__ import annotations

from packaging.version import Version

from kedro_databricks.core.constants import KEDRO_VERSION
from kedro_databricks.core.logger import get_logger

log = get_logger("utils")


def require_databricks_run_script(_version=KEDRO_VERSION) -> bool:
    """Check if the current Kedro version is less than 0.19.8.

    Kedro 0.19.8 introduced a new `run_script` method that is required for
    running tasks on Databricks. This method is not available in earlier
    versions of Kedro. This function checks if the current Kedro version is
    less than 0.19.8.

    Returns:
        bool: whether the current Kedro version is less than 0.19.8
    """
    return _version < Version("0.19.8")


def version_to_str(version: list[int]) -> str:
    if len(version) != 3:  # noqa: PLR2004 - Semantic versioning requires 3 parts
        raise ValueError(f"Invalid version: {version}")
    return ".".join(str(x) for x in version)


def get_arg_value(args: list[str], arg_name: str) -> str | None:
    for i, arg in enumerate(args):
        if "=" in arg:
            _arg, value = arg.split("=", 1)
            if _arg == arg_name:
                return value
        elif arg == arg_name:
            return args[i + 1]
