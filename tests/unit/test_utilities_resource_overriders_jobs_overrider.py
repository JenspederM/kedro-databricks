from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from kedro_databricks.utilities.resource_overrider import JobsResourceOverrider

EXAMPLE_ROOT = Path(__file__).parent.parent.parent / "examples"
EXAMPLES = [p.name for p in EXAMPLE_ROOT.iterdir() if p.is_dir()]


@dataclass
class Example:
    overrides: dict
    resources: dict
    result: dict


def _load_example(name):
    path = EXAMPLE_ROOT / name
    with open(path / "databricks.yml") as f:
        overrides = yaml.safe_load(f)
    with open(path / "resources.yml") as f:
        resources = yaml.safe_load(f)
    with open(path / "result.yml") as f:
        result = yaml.safe_load(f)
    return Example(overrides, resources, result)


@pytest.mark.parametrize("example_name", EXAMPLES)
def test_job_overrider(example_name):
    # Arrange
    example = _load_example(example_name)
    result = {"resources": {"jobs": {}}}
    job_overrider = JobsResourceOverrider()

    # Act
    for job_name, job in example.resources.get("resources", {}).get("jobs", {}).items():
        result["resources"]["jobs"][job_name] = job_overrider.override(
            resource_key=job_name,
            resource=job,
            overrides=example.overrides.get("resources", {}).get("jobs", {}),
            default_key="default",
        )

    # Assert
    assert result == example.result


@pytest.mark.parametrize(
    ["args", "error", "match"],
    [
        ([None, None, {}], ValueError, "resource must be a dictionary"),
        ([None, {}, None], ValueError, "overrides must be a dictionary"),
    ],
)
def test_override_job_fail(args, error, match):
    job_overrider = JobsResourceOverrider()
    with pytest.raises(error, match=match):
        job_overrider.override(*args)
