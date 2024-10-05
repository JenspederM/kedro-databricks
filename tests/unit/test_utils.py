from __future__ import annotations

import subprocess

import pytest
from kedro_databricks.utils import _is_null_or_empty, remove_nulls, run_cmd


def test_run_cmd():
    with pytest.raises(Exception) as e:
        cmd = ["ls", "non_existent_file"]
        run_cmd(cmd)
        assert "Failed to run command" in str(
            e.value
        ), f"Failed to raise exception: {cmd}"
        raise e

    with pytest.raises(Exception) as e:
        cmd = ["ls", "non_existent_file"]
        run_cmd(cmd, msg="Custom message")
        assert "Custom message" in str(e.value), f"Failed to raise exception: {cmd}"
        raise e

    run_cmd(["ls", "non_existent_file"], warn=True)
    run_cmd(["ls", "non_existent_file"], msg="Custom message", warn=True)

    result = run_cmd(["ls", "."])
    assert isinstance(result, subprocess.CompletedProcess)

    result = run_cmd(["ls", "non_existent_file"], msg="Custom message", warn=True)
    assert result is None, result


def test_remove_nulls_from_dict():
    a = {
        "a": 1,
        "b": None,
        "c": {"d": None},
        "e": {"f": {"g": None}},
        "h": {"i": {"j": {}}},
        "k": [],
        "l": [1, 2, 3],
        "m": [1, None, 3],
        "n": [1, {"o": None}, 3],
        "o": [1, [None], 3],
        "p": [1, {"q": {"r": None}}, 3],
    }

    assert remove_nulls(a) == {
        "a": 1,
        "l": [1, 2, 3],
        "m": [1, 3],
        "n": [1, 3],
        "o": [1, 3],
        "p": [1, 3],
    }, remove_nulls(a)


def test_is_null_or_empty():
    assert _is_null_or_empty(None), "Failed to check None"
    assert _is_null_or_empty({}), "Failed to check empty dict"
    assert _is_null_or_empty([]), "Failed to check empty list"
    assert not _is_null_or_empty(1), "Failed to check int"
    assert not _is_null_or_empty("a"), "Failed to check str"
    assert not _is_null_or_empty({1: 1}), "Failed to check dict"
    assert not _is_null_or_empty([1]), "Failed to check list"
