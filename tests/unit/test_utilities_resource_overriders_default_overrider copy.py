import pytest

from kedro_databricks.utilities.resource_overrider import DefaultResourceOverrider


@pytest.mark.parametrize(
    ["args", "error", "match"],
    [
        ([None, None, {}], ValueError, "resource must be a dictionary"),
        ([None, {}, None], ValueError, "overrides must be a dictionary"),
    ],
)
def test_override_job_fail(args, error, match):
    job_overrider = DefaultResourceOverrider()
    with pytest.raises(error, match=match):
        job_overrider.override(*args)


@pytest.mark.parametrize(
    ["actual", "order", "expected"],
    [
        (
            {"b": 2, "a": 1},
            ["a", "b"],
            {"a": 1, "b": 2},
        ),
        (
            {"b": 2, "a": 1},
            None,
            {"a": 1, "b": 2},
        ),
        (
            {"b": 2, "a": 1},
            ["c", "a"],
            {"a": 1, "b": 2},
        ),
        (
            {"b": 2, "a": 1},
            ["c", "d"],
            {"a": 1, "b": 2},
        ),
    ],
)
def test_default_resource_overrider(actual, order, expected):
    result = DefaultResourceOverrider().override(
        resource_key="test_resource",
        resource=actual,
        overrides={},
        default_key="default",
    )
    assert result == expected, result
