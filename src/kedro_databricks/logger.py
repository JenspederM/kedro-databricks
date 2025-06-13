"""Logger configuration for the kedro-databricks package.

This module sets up a logger for the kedro-databricks package, allowing for structured logging
and easier debugging. The logger is configured to log messages at the INFO level by default,
but this can be overridden by setting the `LOG_LEVEL` environment variable.
"""

import logging
import os

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name.

    This function retrieves a logger instance with the given name, which is a child of the root logger
    for the `kedro-databricks` package

    Args:
        name (str): The name of the logger.

    Returns:
        logging.Logger: The logger instance.
    """
    ROOT_LOGGER = logging.getLogger("kedro-databricks")
    return ROOT_LOGGER.getChild(name)
