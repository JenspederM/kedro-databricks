from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from kedro_databricks.init import NODE_TYPE_MAP, InitController
from kedro_databricks.utils import has_databricks_cli
from tests.utils import reset_init


def _write_dummy_catalog(catalog_path: Path):
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


def test_substitute_catalog_paths(metadata):
    controller = InitController(metadata)
    catalog_path = controller.project_path / "conf" / "base" / "catalog.yml"
    _write_dummy_catalog(catalog_path)
    controller.substitute_catalog_paths()
    with open(catalog_path) as f:
        new_catalog = f.read()
    assert "/dbfs/FileStore/fake_project/data" in new_catalog, new_catalog


def test_write_override_template(metadata):
    controller = InitController(metadata)
    default_key = "default"
    provider = "azure"

    controller.write_kedro_databricks_config(default_key, provider)
    override_path = Path(metadata.project_path) / "conf" / "base" / "databricks.yml"
    assert override_path.exists(), "Override template not written"

    with open(override_path) as f:
        override = yaml.safe_load(f)
    assert (
        override.get(default_key) is not None
    ), f"Override template not written: {override}"

    job_cluster = override.get(default_key, {}).get("job_clusters", []).pop()
    assert (
        job_cluster.get("job_cluster_key") == default_key
    ), f"job_cluster_key is wrong: {override}"

    node_type_id = job_cluster.get("new_cluster", {}).get("node_type_id")
    assert node_type_id == NODE_TYPE_MAP[provider], f"node_type_id is wrong: {override}"

    try:
        controller.write_kedro_databricks_config(default_key, provider)
    except Exception:
        pytest.fail("If an override file already exists, it should not be overwritten.")


def test_write_databricks_run_script(metadata):
    controller = InitController(metadata)
    controller.write_databricks_run_script()
    run_script_path = (
        Path(controller.project_path)
        / "src"
        / controller.package_name
        / "databricks_run.py"
    )
    assert run_script_path.exists(), "Databricks run script not written"


def test_bundle_init(metadata):
    controller = InitController(metadata)
    if not has_databricks_cli():
        with pytest.raises(Exception):
            controller.bundle_init()
    else:
        reset_init(metadata)
        controller.bundle_init()
        bundle_path = Path(metadata.project_path) / "databricks.yml"
        if not bundle_path.exists():
            files = [
                f.relative_to(metadata.project_path).as_posix()
                for f in metadata.project_path.iterdir()
            ]
            pytest.fail(
                "Bundle file not written - found files:\n\t{}".format(
                    "\n\t".join(files)
                )
            )

        bundle = yaml.load(bundle_path.read_text(), Loader=yaml.FullLoader)
        assert (
            bundle is not None
        ), f"Bundle template failed to load - {bundle_path.read_text()}"
        assert bundle.get("bundle", {}).get("name") == metadata.package_name, bundle

        try:
            controller.bundle_init()
        except Exception:
            pytest.fail(
                "If a bundle file already exists, it should not be overwritten."
            )
