# kedro-databricks

[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/JenspederM/kedro-databricks/graph/badge.svg?token=0MUFV8BNRH)](https://codecov.io/gh/JenspederM/kedro-databricks)
<a href="https://codeclimate.com/github/JenspederM/kedro-databricks/maintainability"><img src="https://api.codeclimate.com/v1/badges/d5ef60eb0f20cb369b18/maintainability" /></a>
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](https://pypi.org/project/kedro-databricks/)
[![PyPI Version](https://badge.fury.io/py/kedro-databricks.svg)](https://pypi.org/project/kedro-databricks/)
[![Read the Docs](https://app.readthedocs.org/projects/kedro-databricks/badge/?version=latest)](https://kedro-databricks.readthedocs.io/)

Kedro plugin to develop Kedro pipelines for Databricks. This plugin strives to provide the ultimate developer experience when using Kedro on Databricks.

## Key Features

1. **Initialization**: Transform your local Kedro project into a Databricks Asset Bundle.
2. **Generation**: Generate Asset Bundle resources definition based from your kedro pipelines.
3. **Deployment**: Deploy your Kedro pipelines to Databricks as Jobs.
4. **Execution**: Run your Kedro pipelines on Databricks straight from the command line.
5. **Cleanup**: Remove all Databricks resources created by the plugin.

## Documentation & Contributing

To learn more about the plugin, please refer to the [documentation](https://kedro-databricks.readthedocs.io/).

Interested in contributing? Check out our [contribution guidelines](docs/contributing.md) to get started!

## Breaking Changes

### Version `0.14.0`

To accommodate using Databricks Free Edition, we had to change the structure of overrides defined in `conf/<env>/databricks.yml`.

Before:
```
default:
    environments:
        - environment_key: default
    spec:
        environment_version: '4'
        dependencies:
            - ../dist/*.whl
    tasks:
        - task_key: default
          environment_key: default
```

After:
```
resources:
    jobs:
        default:
            environments:
                - environment_key: default
            spec:
                environment_version: '4'
                dependencies:
                    - ../dist/*.whl
            tasks:
                - task_key: default
                environment_key: default
```

This was done so that we could default to creating a volume in a newly initialized `kedro-databricks` project.

While this requires users to migrate their databricks configuration, it also extends the ability of `kedro-databricks` beyond that of applying overrides to specific jobs. Now, you can add any type of resource in your `conf/<env>/databricks.yml` and those will be generated as well.

> NOTE: Merges are only applied for `jobs` currently, so any other defined will be generated as defined in the configuration.

In addition to the changes to the structure of `conf/<env>/databricks.yml`, we now also tag the generated resources with their resource type and target environment, meaning that newly generated resources will be named like `target.<env>.<resource-type>.<resouce-name>.yml`.
