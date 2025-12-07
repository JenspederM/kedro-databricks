from __future__ import annotations

import logging
import platform

import pytest

from kedro_databricks.core.utils import (
    get_arg_value,
    version_to_str,
)

log = logging.getLogger("test")


@pytest.mark.parametrize(
    ["version", "expected", "raises"],
    [
        ([1, 2, 3], "1.2.3", False),
        ([1, 2], "1.2", True),
        ([1], "1", True),
        ([1, 2, 3, 4], "1.2.3.4", True),
    ],
)
def test_version_to_str(version, expected, raises):
    if raises is True:
        with pytest.raises(ValueError):
            version_to_str(version)
    else:
        result = version_to_str(version)
        assert result == expected, f"Expected {expected}, but got {result}"


OS = platform.uname().system.lower()


@pytest.mark.parametrize(
    "args, arg, expected",
    [
        (["--env", "local"], "--env", "local"),
        (["--env", "dev"], "--env", "dev"),
        (["--env", "prod"], "--env", "prod"),
        (["--target", "local"], "--target", "local"),
        (["--target", "dev"], "--target", "dev"),
        (["--target", "prod"], "--target", "prod"),
        (["my-program", "--arg1", "value1", "--arg2", "value2"], "--arg1", "value1"),
        (["my-program", "--arg1", "value1", "--arg2", "value2"], "--arg2", "value2"),
        (["my-program", "--arg1=value1"], "--arg1", "value1"),
    ],
)
def test_get_arg_value(args, arg, expected):
    """Test the function to get the value of a specific argument from a list of arguments."""
    result = get_arg_value(args, arg)
    assert result == expected, f"Expected {expected}, but got {result}"
