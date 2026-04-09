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
    ["actual", "changes", "expected"],
    [
        (
            {"a": 1, "b": 2},
            {"test_resource": {"a": 2, "b": 1}},
            {"a": 2, "b": 1},
        ),
        (
            {"a": 1, "b": 2},
            {"default": {"a": 3}},
            {"a": 3, "b": 2},
        ),
        (
            {"a": 1, "b": 2},
            {"test_resource": {"c": 3}},
            {"a": 1, "b": 2, "c": 3},
        ),
    ],
)
def test_default_resource_overrider(actual, changes, expected):
    print(f"Actual: {actual}")
    print(f"Changes: {changes}")
    result = DefaultResourceOverrider().override(
        resource_key="test_resource",
        resource=actual,
        overrides=changes,
        default_key="default",
    )
    assert result == expected, f"Expected {expected}, but got {result}"
