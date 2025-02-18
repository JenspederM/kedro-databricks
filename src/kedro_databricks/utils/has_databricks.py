import logging
import re
import shutil
import subprocess

MINIMUM_DATABRICKS_VERSION = [0, 205, 0]


def has_databricks_cli() -> bool:
    """Check if the Databricks CLI is installed."""
    if shutil.which("databricks") is None:  # pragma: no cover
        return False
    _check_version()
    return True


def _get_databricks_version() -> list[int]:
    result = subprocess.run(
        ["databricks", "--version"], check=True, capture_output=True
    )
    version_str = re.sub(
        r".*(\d+\.\d+\.\d+)", r"\1", result.stdout.decode("utf-8").strip()
    )
    return list(map(int, version_str.split(".")))


def _to_str(version: list[int]) -> str:
    return ".".join(str(x) for x in version)


def _check_version(log=logging.getLogger("kedro-databricks")):
    intro = "Checking Databricks CLI version"
    log.info(f"{intro}...")
    current_databricks_version = _get_databricks_version()
    if current_databricks_version < MINIMUM_DATABRICKS_VERSION:
        error_msg = f"""{_to_str(current_databricks_version)} < {_to_str(MINIMUM_DATABRICKS_VERSION)}
    Your Databricks CLI version is {_to_str(current_databricks_version)},
    but this script requires at least {_to_str(MINIMUM_DATABRICKS_VERSION)}.
    Visit https://docs.databricks.com/en/dev-tools/cli/install.html to install the latest version.
        """
        raise ValueError(error_msg)
    else:
        log.info(
            f"{intro}: Your Databricks CLI version is {_to_str(current_databricks_version)}"
        )
