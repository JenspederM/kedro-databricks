### Installation

To install the plugin, simply run:

```bash
pip install kedro-databricks
```

Now you can use the plugin to develop Kedro pipelines for Databricks.

### Getting ready

Before you begin, ensure that the Databricks CLI is installed and configured. For more information on installation and configuration, please refer to the [Databricks CLI documentation](https://docs.databricks.com/dev-tools/cli/index.html).

- [Installation Help](https://docs.databricks.com/en/dev-tools/cli/install.html)
- [Configuration Help](https://docs.databricks.com/en/dev-tools/cli/authentication.html)

You can check that the Databricks CLI is configured correctly by running the following command:

```bash
databricks auth describe
```

If the command returns your username and workspace URL, then the Databricks CLI is configured correctly. If you see an error message, please refer to the [Databricks CLI documentation](https://docs.databricks.com/aws/en/dev-tools/cli/authentication) for troubleshooting.

### How to use the plugin

#### Initializing a new Kedro project

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
cd <project-name>  # change directory
```

Install the required dependencies:

```bash
pip install -r requirements.txt
pip install kedro-databricks
```

#### Initializing the Databricks Asset Bundle

Now you can initialize the Databricks asset bundle

```bash
kedro databricks init
```

This command will create the following files:

```
├── databricks.yml # Databricks Asset Bundle configuration
├── conf/
│   └── dev/
│       └── databricks.yml # Workflow overrides
│       └── catalog.yml    # Catalog overrides
│   └── prod/
│       └── databricks.yml # Workflow overrides
│       └── catalog.yml    # Catalog overrides
```

The `databricks.yml` file is the main configuration file for the Databricks Asset Bundle. The `conf/base/databricks.yml` file is used to override the Kedro workflow configuration for Databricks.

Override the Kedro workflow configuration for Databricks in the `conf/<env>/databricks.yml` file:

```yaml
# conf/dev/databricks.yml

default: # will be applied to all workflows
  job_clusters:
    - job_cluster_key: default
      new_cluster:
        spark_version: 15.4.x-scala2.12
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
        spark_version: 15.4.x-scala2.12
        node_type_id: Standard_DS3_v2
        num_workers: 2
        spark_env_vars:
          KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
  tasks:
    - task_key: <my-task> # will only be applied to the specified task in the specified workflow
      job_cluster_key: high-concurrency
```

The plugin loads all configuration named according to `conf/databricks*` or `conf/databricks/*`.

#### Generating bundle resources

Once you have initialized the Databricks Asset Bundle, you can generate the Asset Bundle resources definition. This step is necessary to prepare your Kedro project for deployment to Databricks. Run the following command:

```bash
kedro databricks bundle
```

This command will generate the following files:

```
├── resources/
│   ├── <project>.yml            # corresponds to `kedro run`
│   ├── <project>_<pipeline>.yml # corresponds to `kedro run --pipeline <pipeline>`
```

The generated files contain the Asset Bundle resources definition for your Kedro project, which is necessary for deploying your project to Databricks.

##### Choosing the resource generator

You can choose how resources are generated using `-g/--resource-generator`:

- `node` (default): creates a workflow task for each Kedro node with dependencies.
- `pipeline`: creates a single task that runs the entire pipeline.

You can also provide a fully-qualified dotted path to a custom generator class
that subclasses `kedro_databricks.cli.bundle.resource_generator.AbstractResourceGenerator`.

Examples:

```bash
# Generate per-node tasks (default behavior)
kedro databricks bundle -g node

# Generate a single-task workflow for the whole pipeline
kedro databricks bundle -g pipeline

# Bundle only one pipeline by name
kedro databricks bundle -g pipeline -p my_pipeline

# Pass runtime parameters to tasks
kedro databricks bundle -g node -r "param1=val1,param2=val2"
```

Tip: The same `-g/--resource-generator`, `-p/--pipeline`, and `-r/--params` options are also available when using `kedro databricks deploy --bundle`.

##### Creating a custom resource generator

You can implement your own generator by subclassing
`kedro_databricks.cli.bundle.resource_generator.AbstractResourceGenerator` and
returning a Databricks workflow payload in `_create_workflow_dict`. For example,
the snippet below creates per-node tasks and attaches a custom cluster:

```python
# my_project/generators/custom.py
from __future__ import annotations
from typing import Any

from kedro.pipeline import Pipeline
from kedro_databricks.cli.bundle.resource_generator.abstract_resource_generator import (
    AbstractResourceGenerator,
)


class CustomGenerator(AbstractResourceGenerator):
    """Example generator with a predefined job cluster per task."""

    def _create_workflow_dict(self, name: str, pipeline: Pipeline) -> dict[str, Any]:
        # Your custom logic
        return {"name": name, "tasks": []}
```

Use your custom generator by passing its dotted path with `-g/--resource-generator`:

```bash
kedro databricks bundle -g "my_project.generators.custom.CustomGenerator"
```

#### Deploying to Databricks

With your Kedro project initialized and the Asset Bundle resources generated, you can now deploy your Kedro project to Databricks. Run the following command:

```bash
kedro databricks deploy
```

That's it! Your pipelines have now been deployed as a workflow to Databricks as `[dev <user>] <project_name>`.

#### Running the workflow

To run the workflow on Databricks, you can use the following command:

```bash
kedro databricks run <project_name>
```

It might take a few minutes to run the workflow, depending on the size of your dataset and the complexity of your pipelines. While you wait, you can monitor the progress of your workflow in the Databricks UI.

#### Cleaning up resources

To clean up the resources created by the plugin, you can use the following command:

```bash
kedro databricks destroy
```

This command will remove the Databricks Asset Bundle configuration and any resources created during the deployment process. It is a good practice to clean up resources when they are no longer needed to avoid unnecessary costs.
