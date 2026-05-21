# User Guide

This is the user guide for Kedro-Databricks. It contains detailed instructions on how to use the plugin, including how to customize it for your specific needs.

## Configuration

You can change how `kedro-databricks` works by configuring it as a tool in your `pyproject.toml`. For example:

```toml
# pyproject.toml
[tool.kedro-databricks]
init_schema = "default"
init_catalog = "workspace"
default_env = "dev"
```

The available configuration options are:

::: kedro_databricks.config.Config
    options:
        docstring_section_style: table
        show_labels: false
        show_bases: false
        heading_level: 4

## Resource Generation

You can choose how resources are generated using `-g/--resource-generator`:

- `node` (default): creates a job task for each Kedro node with dependencies.
- `pipeline`: creates a single task that runs the entire pipeline.

You can also provide a fully-qualified dotted path to a custom generator class
that subclasses `kedro_databricks.resource_generator.AbstractResourceGenerator`.

Examples:

```bash
# Generate per-node tasks (default behavior)
kedro databricks bundle -g node

# Generate a single-task job for the whole pipeline
kedro databricks bundle -g pipeline

# Bundle only one pipeline by name
kedro databricks bundle -g pipeline -p my_pipeline

# Pass runtime parameters to tasks
kedro databricks bundle -g node -r "param1=val1,param2=val2"
```

Tip: The same `-g/--resource-generator`, `-p/--pipeline`, and `-r/--params` options are also available when using `kedro databricks deploy --bundle`.

### Creating a custom resource generator

You can implement your own generator by subclassing
`kedro_databricks.resource_generator.AbstractResourceGenerator` and
returning a Databricks job payload in `_create_job_dict`. For example,
the snippet below creates per-node tasks and attaches a custom cluster:

```python
# my_project/generators/custom.py
from __future__ import annotations
from typing import Any

from kedro.pipeline import Pipeline
from kedro_databricks.resource_generator.abstract_resource_generator import (
    AbstractResourceGenerator,
)


class CustomGenerator(AbstractResourceGenerator):
    """Example generator with a predefined job cluster per task."""

    def _create_job_dict(
        self, name: str, pipeline: Pipeline, pipeline_name: str
    ) -> dict[str, Any]:
        # Your custom logic
        return {"name": name, "tasks": []}
```

Use your custom generator by passing its dotted path with `-g/--resource-generator`:

```bash
kedro databricks bundle -g "my_project.generators.custom.CustomGenerator"
```

## Override Behavior

Override files are read from `conf/<env>/databricks.yml` and merged into generated resources.

- `resources.jobs.<job-name>` applies to a specific generated job.
- `resources.jobs.<default-key>` applies to all jobs (default key is `default`, configurable through `--default-key` or project config).
- Regex keys are supported using the configured `regex_prefix` (default `re:`).

Example with job-level regex override:

```yaml
resources:
    jobs:
        default:
            max_concurrent_runs: 1
        re:^my_project_.*:
            tags:
                owner: data-eng
```

Task-level regex overrides are also supported under `resources.jobs.<job>.tasks`:

```yaml
resources:
    jobs:
        my_project_default:
            tasks:
            - task_key: default
                timeout_seconds: 7200
            - task_key: re:^train_.*
                max_retries: 2
```

## Resource Type Support

You can define any Databricks resource type under `resources`.

- `jobs` use specialized merge logic (for tasks, environments, clusters, notifications, health rules, and parameters).
- Other resource types use shallow key-based merging and are written as-is into generated bundle files.

This makes it possible to combine generated jobs with manually maintained resources such as `volumes`, while still using advanced merge behavior for `jobs`.

## Capability Matrix

The table below maps common capabilities to where they are documented and where you can find concrete examples.

| Capability | Main command/options | Primary docs | Example directory |
|---|---|---|---|
| Initialize bundle scaffold | `kedro databricks init` | Getting Started | n/a |
| Reinitialize existing project | `kedro databricks init --overwrite` | Getting Started | n/a |
| Generate node-level jobs | `kedro databricks bundle -g node` | User Guide | `examples/individual_task/` |
| Generate pipeline-level jobs | `kedro databricks bundle -g pipeline` | User Guide | `examples/individual_workflows/` |
| Pass runtime params to tasks | `kedro databricks bundle -r` | User Guide | `examples/individual_task_with_parameters/` |
| Apply defaults and targeted overrides | `--default-key` + `conf/<env>/databricks.yml` | User Guide | `examples/using_default_overrides/` |
| Apply regex overrides (jobs/tasks) | `re:` keys, `--regex-prefix` | User Guide | `examples/regex_overrides/`, `examples/task_regex_overrides/` |
| Use advanced Jobs API fields | override config | User Guide | `examples/with_all_jobs_api_2.2_fields/` |
| Add custom libraries | override config | User Guide | `examples/with_custom_libraries/` |
| Configure health rules | override config | User Guide | `examples/with_health_rules/` |
| Configure job parameters | override config | User Guide | `examples/with_job_parameters/` |
| Configure webhook notifications | override config | User Guide | `examples/webhook_notifications/`, `examples/task_level_webhook_notifications/` |
| Deploy and upload local data | `kedro databricks deploy` | Getting Started | n/a |
| Run jobs on Databricks | `kedro databricks run [pipeline]` | Getting Started | n/a |
| Forward raw Databricks CLI args | `-- ...` | Getting Started | n/a |
| Destroy deployed resources | `kedro databricks destroy` | Getting Started | n/a |
