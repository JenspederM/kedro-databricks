from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import tomlkit
import yaml

import kedro_databricks.commands.init
from kedro_databricks.commands.init import (
    _create_target_configs,
    _prepare_template,
    _substitute_file_path,
    _transform_spark_hook,
    _update_gitignore,
    _write_databricks_run_script,
    command,
)
from kedro_databricks.config import config


@pytest.fixture
def mock_dbcli_init(monkeypatch: pytest.MonkeyPatch, metadata):
    class MockCli:
        def __init__(self, metadata, additional_args):
            self.metadata = metadata

        def init(self, _: Path, __: Path):
            conf = {
                "bundle": {"name": "test"},
                "targets": {"dev": {}, "prod": {}},
                "workspace": {"current_user": {"short_name": "test-user"}},
            }
            with open(metadata.project_path / "databricks.yml", "w") as f:
                yaml.safe_dump(conf, f)
            return conf

    monkeypatch.setattr
    monkeypatch.setattr(kedro_databricks.commands.init, "DatabricksCli", MockCli)


def test_init_mocked(metadata, cli_runner, mock_dbcli_init):
    result = cli_runner.invoke(
        command,
        [],
        obj=metadata,
    )
    assert result.exit_code == 0, (
        result.exit_code,
        result.stdout,
        result.exception,
    )


def test_init_mocked_overwrite(metadata, cli_runner, mock_dbcli_init):
    cli_runner.invoke(
        command,
        [],
        obj=metadata,
    )
    result = cli_runner.invoke(
        command,
        ["--overwrite"],
        obj=metadata,
    )
    assert result.exit_code == 0, (
        result.exit_code,
        result.stdout,
        result.exception,
    )


def test_init_mocked_custom_init(metadata, cli_runner, mock_dbcli_init):
    p = metadata.project_path / "databricks.yml"
    if p.exists():
        p.unlink()
    result = cli_runner.invoke(
        command,
        [
            "--catalog",
            "test",
            "--schema",
            "test",
            "--default-key",
            "test",
            "--env",
            "test",
            "--conf-source",
            "test",
            "--resource-generator",
            "test",
            "--regex-prefix",
            "test:",
        ],
        obj=metadata,
    )
    assert result.exit_code == 0, (
        result.exit_code,
        result.stdout,
        result.exception,
    )

    pyproject_path = metadata.project_path / "pyproject.toml"
    with open(pyproject_path) as f:
        pyproject = tomlkit.load(f)
        for k, v in pyproject.get("tool", {}).get("kedro-databricks", {}).items():
            assert v.startswith("test"), f"{k} does not start with test"


def test_update_gitignore(metadata):
    _update_gitignore(metadata)
    gitignore_path = metadata.project_path / ".gitignore"
    assert gitignore_path.exists(), "Gitignore not written"
    with open(gitignore_path) as f:
        assert ".databricks" in f.read(), "Databricks not in gitignore"


def test_update_gitignore_does_not_exist(metadata):
    if (metadata.project_path / ".gitignore").exists():
        (metadata.project_path / ".gitignore").unlink()
    _update_gitignore(metadata)
    gitignore_path = metadata.project_path / ".gitignore"
    assert gitignore_path.exists(), "Gitignore not written"
    with open(gitignore_path) as f:
        assert ".databricks" in f.read(), ".databricks not in gitignore"


def test_write_databricks_run_script(metadata):
    _write_databricks_run_script(metadata)
    run_script_path = (
        Path(metadata.project_path)
        / "src"
        / metadata.package_name
        / "databricks_run.py"
    )
    assert run_script_path.exists(), "Databricks run script not written"


@pytest.mark.parametrize(
    ["actual", "expected"],
    [
        (
            "file_path: /dbfs/FileStore/develop_eggs/data/01_raw/file.csv",
            "file_path: ${_file_path}/data/01_raw/file.csv",
        ),
        (
            "file_path: /dbfs/develop_eggs/data/01_raw/file.csv",
            "file_path: ${_file_path}/data/01_raw/file.csv",
        ),
        (
            "file_path: /dbfs/FileStore/develop_eggs/data/01_raw/file.csv",
            "file_path: ${_file_path}/data/01_raw/file.csv",
        ),
        (
            "file_path: data/0_raw/file.csv",
            "file_path: ${_file_path}/data/0_raw/file.csv",
        ),
        (
            "file_path: data/012_raw/file.csv",
            "file_path: ${_file_path}/data/012_raw/file.csv",
        ),
        (
            "file_path:data/012_raw/file.csv",
            "file_path: ${_file_path}/data/012_raw/file.csv",
        ),
        (
            "file_path: /custom/path/data/01_raw/file.csv",
            "file_path: ${_file_path}/data/01_raw/file.csv",
        ),
        (
            "file_path: /custom/path/data/01_raw/file.json",
            "file_path: ${_file_path}/data/01_raw/file.json",
        ),
        (
            "file_path: https://website.com/data/file.csv",
            "file_path: https://website.com/data/file.csv",
        ),
        (
            "file_path: https://website.com/data/file.json",
            "file_path: https://website.com/data/file.json",
        ),
        ("data/01_raw/file.csv", "data/01_raw/file.csv"),
    ],
)
def test_substitute_file_path(actual, expected):
    result = _substitute_file_path(actual)
    assert result == expected, f"\n{result}\n{expected}"


def test_create_target_configs(metadata, monkeypatch):
    with open(metadata.project_path / "databricks.yml", "w") as f:
        yaml.safe_dump(
            {
                "bundle": {
                    "name": "develop_eggs",
                },
                "targets": {
                    "dev": {
                        "mode": "development",
                        "workspace": {
                            "host": "https://<your-volume-name>.databricks.com"
                        },
                    }
                },
            },
            f,
        )
    _create_target_configs(
        metadata,
        "test",
        config.init_catalog,
        config.init_schema,
        {"workspace": {"current_user": {"short_name": "test_user"}}},
    )


def test_prepare_template(metadata):
    assets_dir, template_params = _prepare_template(metadata)
    assert assets_dir.exists(), "Assets directory not created"
    params = template_params.read_text()
    try:
        conf = json.loads(params)
        assert conf["project_name"] == metadata.package_name, "Project name not set"
        assert conf["project_slug"] == metadata.package_name, "Package name not set"
    except Exception as e:
        raise ValueError(f"Failed to load template params: {e} - {params}")
    shutil.rmtree(assets_dir)


def test_substitute_catalog():
    catalog = """
_file_path: /Volumes/workspace/default/volume/develop_eggs
# Here you can define all your data sets by using simple YAML syntax.
#
# Documentation for this file format can be found in "The Data Catalog"
# Link: https://docs.kedro.org/en/stable/data/data_catalog.html
#
# We support interacting with a variety of data stores including local file systems, cloud, network and HDFS
#
# An example data set definition can look as follows:
#
#bikes:
#  type: pandas.CSVDataset
#  filepath: "data/01_raw/bikes.csv"
#
#weather:
#  type: spark.SparkDatasetV2
#  filepath: s3a://your_bucket/data/01_raw/weather*
#  file_format: csv
#  credentials: dev_s3
#  load_args:
#    header: True
#    inferSchema: True
#  save_args:
#    sep: '|'
#    header: True
#
#scooters:
#  type: pandas.SQLTableDataset
#  credentials: scooters_credentials
#  table_name: scooters
#  load_args:
#    index_col: ['name']
#    columns: ['name', 'gear']
#  save_args:
#    if_exists: 'replace'
#    # if_exists: 'fail'
#    # if_exists: 'append'
#
# The Data Catalog supports being able to reference the same file using two different Dataset implementations
# (transcoding), templating and a way to reuse arguments that are frequently repeated. See more here:
# https://docs.kedro.org/en/stable/data/data_catalog.html
#
# This is a data set used by the iris classification example pipeline provided with this starter
# template. Please feel free to remove it once you remove the example pipeline.

example_iris_data:
  type: spark.SparkDatasetV2
  filepath: /dbfs/FileStore/develop_eggs/data/01_raw/iris.csv
  file_format: csv
  load_args:
    header: True
    inferSchema: True
  save_args:
    header: True

# We need to set mode to 'overwrite' in save_args so when saving the dataset it is replaced each time it is run
# for all SparkDatasetV2s.
X_train@pyspark:
  type: spark.SparkDatasetV2
  filepath: /dbfs/FileStore/develop_eggs/data/02_intermediate/X_train.parquet
  save_args:
    mode: overwrite

X_train@pandas:
  type: pandas.ParquetDataset
  filepath: /dbfs/FileStore/develop_eggs/data/02_intermediate/X_train.parquet

X_test@pyspark:
  type: spark.SparkDatasetV2
  filepath: /dbfs/FileStore/develop_eggs/data/02_intermediate/X_test.parquet
  save_args:
    mode: overwrite

X_test@pandas:
  type: pandas.ParquetDataset
  filepath: /dbfs/FileStore/develop_eggs/data/02_intermediate/X_test.parquet

y_train@pyspark:
  type: spark.SparkDatasetV2
  filepath: /dbfs/FileStore/develop_eggs/data/02_intermediate/y_train.parquet
  save_args:
    mode: overwrite

y_train@pandas:
  type: pandas.ParquetDataset
  filepath: /dbfs/FileStore/develop_eggs/data/02_intermediate/y_train.parquet

y_test@pyspark:
  type: spark.SparkDatasetV2
  filepath: /dbfs/FileStore/develop_eggs/data/02_intermediate/y_test.parquet
  save_args:
    mode: overwrite

y_test@pandas:
  type: pandas.ParquetDataset
  filepath: /dbfs/FileStore/develop_eggs/data/02_intermediate/y_test.parquet

y_pred:
  type: pandas.ParquetDataset
  filepath: ${_file_path}/data/03_primary/y_pred.parquet
"""
    expected = """
_file_path: /Volumes/workspace/default/volume/develop_eggs
# Here you can define all your data sets by using simple YAML syntax.
#
# Documentation for this file format can be found in "The Data Catalog"
# Link: https://docs.kedro.org/en/stable/data/data_catalog.html
#
# We support interacting with a variety of data stores including local file systems, cloud, network and HDFS
#
# An example data set definition can look as follows:
#
#bikes:
#  type: pandas.CSVDataset
#  filepath: "data/01_raw/bikes.csv"
#
#weather:
#  type: spark.SparkDatasetV2
#  filepath: s3a://your_bucket/data/01_raw/weather*
#  file_format: csv
#  credentials: dev_s3
#  load_args:
#    header: True
#    inferSchema: True
#  save_args:
#    sep: '|'
#    header: True
#
#scooters:
#  type: pandas.SQLTableDataset
#  credentials: scooters_credentials
#  table_name: scooters
#  load_args:
#    index_col: ['name']
#    columns: ['name', 'gear']
#  save_args:
#    if_exists: 'replace'
#    # if_exists: 'fail'
#    # if_exists: 'append'
#
# The Data Catalog supports being able to reference the same file using two different Dataset implementations
# (transcoding), templating and a way to reuse arguments that are frequently repeated. See more here:
# https://docs.kedro.org/en/stable/data/data_catalog.html
#
# This is a data set used by the iris classification example pipeline provided with this starter
# template. Please feel free to remove it once you remove the example pipeline.

example_iris_data:
  type: spark.SparkDatasetV2
  filepath: ${_file_path}/data/01_raw/iris.csv
  file_format: csv
  load_args:
    header: True
    inferSchema: True
  save_args:
    header: True

# We need to set mode to 'overwrite' in save_args so when saving the dataset it is replaced each time it is run
# for all SparkDatasetV2s.
X_train@pyspark:
  type: spark.SparkDatasetV2
  filepath: ${_file_path}/data/02_intermediate/X_train.parquet
  save_args:
    mode: overwrite

X_train@pandas:
  type: pandas.ParquetDataset
  filepath: ${_file_path}/data/02_intermediate/X_train.parquet

X_test@pyspark:
  type: spark.SparkDatasetV2
  filepath: ${_file_path}/data/02_intermediate/X_test.parquet
  save_args:
    mode: overwrite

X_test@pandas:
  type: pandas.ParquetDataset
  filepath: ${_file_path}/data/02_intermediate/X_test.parquet

y_train@pyspark:
  type: spark.SparkDatasetV2
  filepath: ${_file_path}/data/02_intermediate/y_train.parquet
  save_args:
    mode: overwrite

y_train@pandas:
  type: pandas.ParquetDataset
  filepath: ${_file_path}/data/02_intermediate/y_train.parquet

y_test@pyspark:
  type: spark.SparkDatasetV2
  filepath: ${_file_path}/data/02_intermediate/y_test.parquet
  save_args:
    mode: overwrite

y_test@pandas:
  type: pandas.ParquetDataset
  filepath: ${_file_path}/data/02_intermediate/y_test.parquet

y_pred:
  type: pandas.ParquetDataset
  filepath: ${_file_path}/data/03_primary/y_pred.parquet
"""
    assert _substitute_file_path(catalog) == expected


def test_transform_spark_hook(tmp_path_factory):
    # Arrange
    tmpdir = tmp_path_factory.mktemp("kedro-databricks")
    hook_file = tmpdir / "spark_hook.py"
    hook_file.write_text(
        """from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkHooks:

    @hook_impl
    def after_context_created(self, context) -> None:
        \"\"\"Initialises a SparkSession using the config
        defined in project's conf folder.
        \"\"\"

        # Load the spark configuration in spark.yaml using the config loader
        parameters = context.config_loader["spark"]
        spark_conf = SparkConf().setAll(parameters.items())

        # Initialise the spark session
        spark_session_conf = (
            SparkSession.builder.appName(context.project_path.name)
            .enableHiveSupport()
            .config(conf=spark_conf)
        )
        _spark_session = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")
"""
    )

    # Act
    _transform_spark_hook(hook_file.as_posix())

    # Assert
    expected = """from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession

class SparkHooks:

    @hook_impl
    def after_context_created(self, context) -> None:
        \"\"\"Initialises a SparkSession using the config
        defined in project's conf folder.
        \"\"\"
        if context.env != 'local':
            return
        parameters = context.config_loader['spark']
        spark_conf = SparkConf().setAll(parameters.items())
        spark_session_conf = SparkSession.builder.appName(context.project_path.name).enableHiveSupport().config(conf=spark_conf)
        _spark_session = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel('WARN')"""

    assert hook_file.read_text() == expected
