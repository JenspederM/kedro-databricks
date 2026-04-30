import pytest
from pydantic import ValidationError

from kedro_databricks.config import Config


def test_config_default(tmp_path_factory):
    tmpdir = tmp_path_factory.mktemp("kedro-databricks")
    pyproject_toml = tmpdir / "pyproject.toml"
    pyproject_toml.write_text("")
    settings = Config()
    assert settings.init_catalog == "workspace"
    assert settings.init_schema == "default"
    assert settings.default_env == "dev"
    assert settings.workflow_default_key == "default"
    assert settings.workflow_generator == "node"
    assert settings.regex_prefix == "re:"


def test_config_from_pyproject(tmp_path_factory, monkeypatch):
    tmpdir = tmp_path_factory.mktemp("kedro-databricks")
    pyproject_toml = tmpdir / "pyproject.toml"
    catalog = "my-catalog"
    schema = "my-schema"
    env = "my-env"
    workflow_default_key = "my-default"
    workflow_generator = "my-generator"
    regex_prefix = "my-prefix:"
    pyproject_toml.write_text(f"""
[tool.kedro-databricks]
init_catalog='{catalog}'
init_schema='{schema}'
default_env='{env}'
workflow_default_key='{workflow_default_key}'
workflow_generator='{workflow_generator}'
regex_prefix='{regex_prefix}'
""")
    monkeypatch.chdir(tmpdir)
    settings = Config()
    assert settings.init_catalog == catalog
    assert settings.init_schema == schema
    assert settings.default_env == env
    assert settings.workflow_default_key == workflow_default_key
    assert settings.workflow_generator == workflow_generator
    assert settings.regex_prefix == regex_prefix


def test_config_invalid_regex_prefix(tmp_path_factory, monkeypatch):
    tmpdir = tmp_path_factory.mktemp("kedro-databricks")
    pyproject_toml = tmpdir / "pyproject.toml"
    pyproject_toml.write_text("""
[tool.kedro-databricks]
regex_prefix='my-prefix='
""")
    monkeypatch.chdir(tmpdir)
    with pytest.raises(ValidationError, match="does not end in ':'"):
        Config()


def test_config_invalid_catalog(tmp_path_factory, monkeypatch):
    tmpdir = tmp_path_factory.mktemp("kedro-databricks")
    pyproject_toml = tmpdir / "pyproject.toml"

    pyproject_toml.write_text("""
[tool.kedro-databricks]
init_catalog='my.test'
""")
    monkeypatch.chdir(tmpdir)
    with pytest.raises(ValidationError, match="cannot contain '.'"):
        Config()


def test_config_invalid_schema(tmp_path_factory, monkeypatch):
    tmpdir = tmp_path_factory.mktemp("kedro-databricks")
    pyproject_toml = tmpdir / "pyproject.toml"

    pyproject_toml.write_text("""
[tool.kedro-databricks]
init_schema='my.test'
""")
    monkeypatch.chdir(tmpdir)
    with pytest.raises(ValidationError, match="cannot contain '.'"):
        Config()
