
# Getting Started
### Prerequisites

Before you begin, ensure that the Databricks CLI is installed and configured. For more information on installation and configuration, please refer to the [Databricks CLI documentation](https://docs.databricks.com/dev-tools/cli/index.html).

- [Installation Help](https://docs.databricks.com/en/dev-tools/cli/install.html)
- [Configuration Help](https://docs.databricks.com/en/dev-tools/cli/authentication.html)

### How to use the plugin

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

Now you can initialize the Databricks asset bundle

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

```bash
kedro databricks run <project_name>
```

It might take a few minutes to run the workflow, depending on the size of your dataset and the complexity of your pipelines.

You can clean up the resources created by the plugin by running:

```bash
kedro databricks destroy
```
