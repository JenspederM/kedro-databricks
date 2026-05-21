# Getting Started

## Prerequisites

### Installing the Plugin

To install the plugin, simply run:

```bash
pip install kedro-databricks
```

Now you can use the plugin to develop Kedro pipelines for Databricks.

### Authenticating with Databricks

Before you begin, ensure that the Databricks CLI is installed and configured. For more information on installation and configuration, please refer to the [Databricks CLI documentation](https://docs.databricks.com/dev-tools/cli/index.html).

- [Installation Help](https://docs.databricks.com/en/dev-tools/cli/install.html)
- [Configuration Help](https://docs.databricks.com/en/dev-tools/cli/authentication.html)

You can check that the Databricks CLI is configured correctly by running the following command:

```bash
databricks auth describe
```

If the command returns your username and workspace URL, then the Databricks CLI is configured correctly. If you see an error message, please refer to the [Databricks CLI documentation](https://docs.databricks.com/aws/en/dev-tools/cli/authentication) for troubleshooting.

## Using Kedro-Databricks

### Creating a New Kedro Project

To create a new project, [ensure you have installed Kedro into a virtual environment](https://docs.kedro.org/en/stable/get_started/install.html). Then use the following command:

```bash
pip install kedro
```

You can initialize the `databricks-iris` starter with the following command:

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

### Initializing the Databricks Asset Bundle

Now you can initialize the Databricks asset bundle

```bash
kedro databricks init
```

This command will create the following files:

```
├── databricks.yml # Databricks Asset Bundle configuration
├── conf/
│   └── dev/
│       └── databricks.yml # Resource overrides
│       └── catalog.yml    # Catalog overrides
│   └── prod/
│       └── databricks.yml # Resource overrides
│       └── catalog.yml    # Catalog overrides
```

The `databricks.yml` file is the main configuration file for the Databricks Asset Bundle. Environment-specific overrides are defined in `conf/<env>/databricks.yml` (for example `conf/dev/databricks.yml`).

Override the Kedro resource configuration for Databricks in the `conf/<env>/databricks.yml` file:

```yaml
# conf/dev/databricks.yml

resources:
  jobs:
    default: # will be applied to all jobs
      environments:
      - environment_key: default
        spec:
          dependencies:
          - ../dist/*.whl
          environment_version: '4'
      tasks:
      - environment_key: default
        task_key: default
      - environment_key: my-task # will only be applied to the task named `my-task`
        task_key: default
    my-job: # will only be applied to the job named `my-job`
      environments:
      - environment_key: my-job
        spec:
          dependencies:
          - ../dist/*.whl
          environment_version: '4'
      tasks:
      - environment_key: default
        task_key: default
  volumes:
    my_volume:
      catalog_name: workspace
      comment: A volume to store my data
      grants:
      - principal: \${workspace.current_user.userName} # We can use Databricks Asset bundle substitutions
        privileges:
        - READ_VOLUME
        - WRITE_VOLUME
      name: my_volume
      schema_name: default
      volume_type: MANAGED
```

The plugin loads Databricks config patterns from your Kedro config loader using `databricks*` and `databricks/**`.

In practice, this means files like these are discovered:

- `conf/base/databricks.yml`
- `conf/<env>/databricks.yml`
- `conf/<env>/databricks/<name>.yml`

Useful options for initialization:

- `--catalog` and `--schema` control the default Unity Catalog location used in generated target catalog files.
- `--overwrite` allows re-initializing an existing project (for example when you want to regenerate `databricks.yml`).
- `--default-key` controls the default override key name.
- `--regex-prefix` controls the prefix used for regex-based overrides (default: `re:`).

### Generating Pipeline Resources

Once you have initialized the Databricks Asset Bundle, you can generate the Asset Bundle resources definition. This step is necessary to prepare your Kedro project for deployment to Databricks. Run the following command:

```bash
kedro databricks bundle
```

This command will generate the following files:

```
├── resources/
│   ├── target.<env>.<resource-type>.<resource-name>.yml  # We support any databricks resource type
│   ├── target.<env>.jobs.<project>.yml                   # corresponds to `kedro run`
│   ├── target.<env>.jobs.<project>_<pipeline>.yml        # corresponds to `kedro run --pipeline <pipeline>`
```

The generated files contain the Asset Bundle resources definition for your Kedro project, which is necessary for deploying your project to Databricks.

Useful bundle options:

- `-g/--resource-generator`: `node` (default) or `pipeline`.
- `-p/--pipeline`: generate only a specific pipeline.
- `-r/--params`: pass runtime parameters to generated tasks.
- `--default-key`: override key used as default values during merge.
- `--overwrite`: overwrite existing generated files.

Note: the `node` generator requires datasets to be explicitly defined in your catalog. If your pipeline relies on implicit `MemoryDataset` values, use `-g pipeline` or define the missing datasets.

!!! warning "Node Generator Limitation: `MemoryDataset`"

    The `node` resource generator validates that pipeline inputs/outputs are not implicit `MemoryDataset` values.

    If undeclared datasets are found, bundling fails with guidance to:

    - define datasets explicitly in your catalog,
    - switch to `-g pipeline`,
    - or implement a custom generator as described in the [User Guide](user-guide.md#creating-a-custom-resource-generator).

    This check helps prevent generating node-level Databricks tasks that cannot be executed reliably due to missing persisted datasets.

### Deploying to Databricks

With your Kedro project initialized and the Asset Bundle resources generated, you can now deploy your Kedro project to Databricks. Run the following command:

```bash
kedro databricks deploy
```

If you want deploy to regenerate resources first, use:

```bash
kedro databricks deploy --bundle
```

During deploy, the plugin also uploads your local `data/` directory (if present) to DBFS under the target-specific `_file_path` from `conf/<env>/catalog.yml`.

That's it! Your pipelines have now been deployed as a job to Databricks as `[dev <user>] <project_name>`.

### Running Your Pipelines on Databricks

To run the job on Databricks, you can use the following command:

```bash
# Run the default job (package-level pipeline)
kedro databricks run

# Run a specific pipeline/job
kedro databricks run <pipeline_name>
```

It might take a few minutes to run the job, depending on the size of your dataset and the complexity of your pipelines. While you wait, you can monitor the progress of your job in the Databricks UI.

### Passing Databricks CLI Arguments

All operational commands support forwarding extra Databricks CLI arguments by placing them after `--`.

Examples:

```bash
# Deploy to the configured environment with a specific Databricks profile
kedro databricks deploy -- --profile PROD

# Run using explicit target/profile settings
kedro databricks run my_pipeline -- --target prod --profile PROD

# Destroy with forwarded Databricks CLI options
kedro databricks destroy -- --profile PROD
```

### Cleaning up Your Resources

To clean up the resources created by the plugin, you can use the following command:

```bash
kedro databricks destroy
```

This command will remove the Databricks Asset Bundle configuration and any resources created during the deployment process. It is a good practice to clean up resources when they are no longer needed to avoid unnecessary costs.

## Troubleshooting

### Databricks CLI not found or too old

If commands fail before running bundle operations, verify your Databricks CLI installation and version:

```bash
databricks --version
```

Install or upgrade using the Databricks CLI docs:

- [Install Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html)

### Bundle generation fails due to missing datasets

If node-level generation fails with `MemoryDataset`-related errors, either:

- define the missing datasets explicitly in your catalog, or
- switch to pipeline-level generation:

```bash
kedro databricks bundle -g pipeline
```

### Deploy does not upload data

Deploy only uploads local `data/` if all of the following are true:

- the `data/` directory exists,
- `conf/<env>/catalog.yml` exists,
- and `catalog.yml` contains `_file_path`.

If upload is skipped, check these files first.

### Need explicit Databricks target/profile

Forward Databricks CLI options after `--`:

```bash
kedro databricks deploy -- --target prod --profile PROD
```

## Next Steps

Now that you have successfully deployed your Kedro project to Databricks, you can explore the [User Guide](user-guide.md) for more detailed instructions on how to customize and extend your deployment. Alternatively, you can check out the [CLI Reference](reference/cli.md) to learn more about the available commands and options. If you want to contribute to the project, please refer to the [Contributing Guide](contributing.md) for guidelines on how to get involved.
