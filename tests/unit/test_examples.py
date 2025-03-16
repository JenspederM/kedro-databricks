from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from kedro_databricks.utils.override_resources import override_resources

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
def test_examples(example_name):
    example = _load_example(example_name)
    result = override_resources(example.resources, example.overrides, "default")
    assert result == example.result
