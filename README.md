# kedro-databricks

[![Rye](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/rye/main/artwork/badge.json)](https://rye.astral.sh)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/JenspederM/kedro-databricks/graph/badge.svg?token=0MUFV8BNRH)](https://codecov.io/gh/JenspederM/kedro-databricks)
<a href="https://codeclimate.com/github/JenspederM/kedro-databricks/maintainability"><img src="https://api.codeclimate.com/v1/badges/d5ef60eb0f20cb369b18/maintainability" /></a>
[![Python Version](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue.svg)](https://pypi.org/project/kedro-databricks/)
[![PyPI Version](https://badge.fury.io/py/kedro-databricks.svg)](https://pypi.org/project/kedro-databricks/)


Kedro plugin to develop Kedro pipelines for Databricks. This plugin strives to provide the ultimate developer experience when using Kedro on Databricks. The plugin provides three main features:

1. **Initialization**: Transform your local Kedro project into a Databricks Asset Bundle project with a single command.
2. **Generation**: Generate Asset Bundle resources definition with a single command.
3. **Deployment**: Deploy your Kedro project to Databricks with a single command.

# Installation

To install the plugin, simply run:

```bash
pip install kedro-databricks
```

Now you can use the plugin to develop Kedro pipelines for Databricks.

# How to get started

## Prerequisites:

Before you begin, ensure that the Databricks CLI is installed and configured. For more information on installation and configuration, please refer to the [Databricks CLI documentation](https://docs.databricks.com/dev-tools/cli/index.html).

- [Installation Help](https://docs.databricks.com/en/dev-tools/cli/install.html)
- [Configuration Help](https://docs.databricks.com/en/dev-tools/cli/authentication.html)

## Creating a new project

To create a project based on this starter, [ensure you have installed Kedro into a virtual environment](https://docs.kedro.org/en/stable/get_started/install.html). Then use the following command:

```bash
pip install kedro
```

Soon you will be able to initialize the `databricks-iris` starter with the following command:

```bash
kedro new --starter="databricks-iris"
```

After the project is created, navigate to the newly created project directory:

```bash
cd <my-project-name>  # change directory
```

Install the required dependencies:

```bash
pip install -r requirements.txt
pip install kedro-databricks
```

Now you can nitialize the Databricks asset bundle

```bash
kedro databricks init
```

Next, generate the Asset Bundle resources definition:

```bash
kedro databricks bundle
```

Finally, deploy the Kedro project to Databricks:

```bash
kedro databricks deploy
```

That's it! Your pipelines have now been deployed as a workflow to Databricks as `[dev <user>] <project_name>`. Try running the workflow to see the results.

## Commands

### `kedro databricks init`

To initialize a Kedro project for Databricks, run:

```bash
kedro databricks init
```

This command will create the following files:

```
├── databricks.yml # Databricks Asset Bundle configuration
├── conf/
│   └── base/
│       └── databricks.yml # Workflow overrides
```

The `databricks.yml` file is the main configuration file for the Databricks Asset Bundle. The `conf/base/databricks.yml` file is used to override the Kedro workflow configuration for Databricks.

Override the Kedro workflow configuration for Databricks in the `conf/base/databricks.yml` file:

```yaml
# conf/base/databricks.yml

default: # will be applied to all workflows
    job_clusters:
        - job_cluster_key: default
          new_cluster:
            spark_version: 7.3.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 2
            spark_env_vars:
                KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
    tasks: # will be applied to all tasks in each workflow
        - task_key: default
          job_cluster_key: default

<workflow-name>: # will only be applied to the workflow with the specified name
    job_clusters:
        - job_cluster_key: high-concurrency
          new_cluster:
            spark_version: 7.3.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 2
            spark_env_vars:
                KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
    tasks:
        - task_key: default # will be applied to all tasks in the specified workflow
          job_cluster_key: high-concurrency
        - task_key: <my-task> # will only be applied to the specified task in the specified workflow
          job_cluster_key: high-concurrency
```

The plugin loads all configuration named according to `conf/databricks*` or `conf/databricks/*`.

### `kedro databricks bundle`

To generate Asset Bundle resources definition, run:

```bash
kedro databricks bundle
```

This command will generate the following files:

```
├── resources/
│   ├── <project>.yml # Asset Bundle resources definition corresponds to `kedro run`
│   └── <project-pipeline>.yml # Asset Bundle resources definition for each pipeline corresponds to `kedro run --pipeline <pipeline-name>`
```

The generated resources definition files are used to define the resources required to run the Kedro pipeline on Databricks.

### `kedro databricks deploy`

To deploy a Kedro project to Databricks, run:

```bash
kedro databricks deploy
```

This command will deploy the Kedro project to Databricks. The deployment process includes the following steps:

1. Package the Kedro project for a specfic environment
2. Generate Asset Bundle resources definition for that environment
3. Upload environment-specific `/conf` files to Databricks
4. Upload `/data/raw/*` and ensure other `/data` directories are created
5. Deploy Asset Bundle to Databricks
