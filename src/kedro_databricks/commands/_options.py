import click
from kedro.framework.cli.project import (
    CONF_SOURCE_HELP,
    PARAMS_ARG_HELP,
    PIPELINE_ARG_HELP,
)
from kedro.framework.cli.utils import ENV_HELP

from kedro_databricks.config import config

default_key = click.option(
    "--default-key",
    type=str,
    default=config.workflow_default_key,
    help="Key to use for default overrides in `kedro databricks bundle`",
)

default_env = click.option(
    "-e",
    "--default_env",
    default=config.default_env,
    help=ENV_HELP,
)

overwrite = click.option(
    "--overwrite",
    default=False,
    is_flag=True,
    show_default=True,
    help="Overwrite existing initialization",
)

resource_generator = click.option(
    "-g",
    "--resource-generator",
    default=config.workflow_generator,
    help="Generator used to create resources. Options are 'node' (create a job for each node) or 'pipeline' (create a single job for the entire pipeline).",
)

env = click.option(
    "-e",
    "--env",
    default=config.default_env,
    help=ENV_HELP,
)

conf_source = click.option(
    "-c",
    "--conf-source",
    default=config.conf_source,
    help=CONF_SOURCE_HELP,
)

pipeline = click.option(
    "-p",
    "--pipeline",
    default=None,
    help=PIPELINE_ARG_HELP,
)

params = click.option(
    "-r",
    "--params",
    default=None,
    help=PARAMS_ARG_HELP,
)

runtime_params = click.option(
    "--runtime-params",
    default=None,
    help=PARAMS_ARG_HELP + " (forwarded to the bundle command).",
    deprecated=True,
)

catalog = click.option(
    "--catalog",
    type=str,
    default=config.init_catalog,
    help="Set the catalog for Databricks targets",
)

schema = click.option(
    "--schema",
    type=str,
    default=config.init_schema,
    help="Set the schema for Databricks targets",
)

bundle = click.option(
    "-b",
    "--bundle/--no-bundle",
    default=False,
    help="Bundle the project before deploying",
)

pipeline_arg = click.argument(
    "pipeline",
    default="",
    nargs=1,
)

databricks_args = click.argument(
    "databricks_args",
    nargs=-1,
    type=click.UNPROCESSED,
)
