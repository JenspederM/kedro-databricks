---
title: Kedro Databricks
---

<p align="center">
  <img src="assets/kedro-databricks-logo.png" width="350" title="kedro-databricks logo">
</p>

[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/JenspederM/kedro-databricks/graph/badge.svg?token=0MUFV8BNRH)](https://codecov.io/gh/JenspederM/kedro-databricks)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](https://pypi.org/project/kedro-databricks/)
[![Download/Month](https://img.shields.io/pypi/dm/kedro-databricks)](https://pypi.org/project/kedro-databricks/)
[![PyPI Version](https://badge.fury.io/py/kedro-databricks.svg)](https://pypi.org/project/kedro-databricks/)
[![Read the Docs](https://app.readthedocs.org/projects/kedro-databricks/badge/?version=latest)](https://kedro-databricks.readthedocs.io/)


Kedro plugin to develop Kedro pipelines for Databricks. This plugin strives to provide an excellent developer experience when using Kedro on Databricks.

## Key Features

1. **Initialization**: Transform your local Kedro project into a Databricks Asset Bundle.
2. **Generation**: Generate Asset Bundle resource definitions from your Kedro pipelines.
3. **Deployment**: Deploy your Kedro pipelines to Databricks as Jobs.
4. **Execution**: Run your Kedro pipelines on Databricks straight from the command line.
5. **Cleanup**: Remove all Databricks resources created by the plugin.

## Advanced Capabilities

- Resource generation modes (`node` and `pipeline`) with support for custom generators.
- Flexible override model with defaults, named overrides, and regex-based overrides.
- Support for non-job resources (for example volumes) in `conf/<env>/databricks.yml`.
- Databricks CLI passthrough (`-- ...`) for advanced target/profile control.
- Automatic local data upload during deploy when `_file_path` is configured.

For practical examples and deep-dive configuration patterns, see the [User Guide](user-guide.md).
