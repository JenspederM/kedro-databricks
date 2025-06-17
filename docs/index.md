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
<a href="https://codeclimate.com/github/JenspederM/kedro-databricks/maintainability"><img src="https://api.codeclimate.com/v1/badges/d5ef60eb0f20cb369b18/maintainability" /></a>
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://pypi.org/project/kedro-databricks/)
[![PyPI Version](https://badge.fury.io/py/kedro-databricks.svg)](https://pypi.org/project/kedro-databricks/)
[![Read the Docs](https://app.readthedocs.org/projects/kedro-databricks/badge/?version=latest)](https://kedro-databricks.readthedocs.io/)


Kedro plugin to develop Kedro pipelines for Databricks. This plugin strives to provide the ultimate developer experience when using Kedro on Databricks.

## Key Features

1. **Initialization**: Transform your local Kedro project into a Databricks Asset Bundle.
2. **Generation**: Generate Asset Bundle resources definition based from your kedro pipelines.
3. **Deployment**: Deploy your Kedro pipelines to Databricks as Workflows.
4. **Execution**: Run your Kedro pipelines on Databricks straight from the command line.
5. **Cleanup**: Remove all Databricks resources created by the plugin.
