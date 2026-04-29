from __future__ import annotations

from importlib import metadata, resources
from typing import Annotated

from packaging.version import Version
from pydantic import AfterValidator, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    PyprojectTomlConfigSettingsSource,
    SettingsConfigDict,
)

TEMPLATES = resources.files("kedro_databricks").joinpath("templates")

KEDRO_VERSION = Version(metadata.version("kedro"))
"""Kedro version used to build this plugin."""

MINIMUM_DATABRICKS_VERSION = [0, 205, 0]
"""Minimum Databricks version required for this plugin."""

MAX_TASK_KEY_LENGTH = 100
"""Maximum number of characters in a task key in Databricks jobs."""


def no_dot_in_name(value: str):
    if "." in value:
        raise ValueError(f"{value} cannot contain '.'")
    return value


def regex_prefix_ending(value: str):
    if not value.endswith(":"):
        raise ValueError(f"{value} does not end in ':'")
    return value


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="KEDRO_DATABRICKS_",
        pyproject_toml_table_header=("tool", "kedro-databricks"),
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            PyprojectTomlConfigSettingsSource(settings_cls),
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )

    init_catalog: Annotated[str, AfterValidator(no_dot_in_name)] = Field(
        default="workspace"
    )
    "Default catalog for `kedro databricks init`"

    init_schema: Annotated[str, AfterValidator(no_dot_in_name)] = Field(
        default="default"
    )
    "Default schema for `kedro databricks init`"

    conf_source: str = Field(default="conf")
    "Path of a directory where project configuration is stored"

    default_env: str = Field(default="dev")
    "Default target environment for `kedro-databricks` commands."

    workflow_default_key: str = Field(default="default")
    "Default key to use for overrides in `kedro databricks bundle`"

    workflow_generator: str = Field(default="node")
    "Default generator to use for generating Databricks Asset Bundle resources"

    regex_prefix: Annotated[str, AfterValidator(regex_prefix_ending)] = Field(
        default="re:"
    )
    "Prefix to use for discovering regex workflow or task overrides. Must end in ':'"


config = Config()  # pragma: no cover
