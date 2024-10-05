from __future__ import annotations

from pathlib import Path

import yaml
from kedro_databricks.init import substitute_catalog_paths, write_override_template


def test_substitute_catalog_paths(metadata):
    catalog_path = metadata.project_path / "conf" / "base" / "catalog.yml"
    catalog = {
        "test": {
            "type": "pandas.CSVDataSet",
            "filepath": "/dbfs/FileStore/my-project/data/01_raw/test.csv",
        },
        "test2": {
            "type": "pandas.CSVDataSet",
            "filepath": "/dbfs/FileStore/my-project/data/01_raw/test2.csv",
        },
    }
    with open(catalog_path, "w") as f:
        f.write(yaml.dump(catalog))
    substitute_catalog_paths(metadata)
    with open(catalog_path) as f:
        new_catalog = f.read()
    assert "/dbfs/FileStore/fake_project/data" in new_catalog, new_catalog


def test_write_override_template(metadata):
    override_path = Path(metadata.project_path) / "conf" / "base" / "databricks.yml"
    write_override_template(metadata, "default", "azure")
    assert override_path.exists(), "Override template not written"
