# Examples

## broken_custom_task

This example is related to a specific issue in the Kedro Databricks repository where certain fields did not get properly overridden in the `kedro_databricks` configuration. The example demonstrates how to set up a custom task that uses the `kedro_databricks` package, but it intentionally includes a broken configuration to illustrate the issue.

This issue has been fixed in the `kedro-databricks` package, and the example serves as a reference for users who may encounter similar issues in their own projects.

===! "conf/`[env]`/databricks.yml"
    ```yaml
    resources:
      jobs:
        my_pipeline:
          tasks:
            - task_key: default
              package_name: my_package
            - task_key: taskB
              run_if: AT_LEAST_ONE_SUCCESS
    ```

=== "Before: resources/`[pipeline]`.yml"
    ```yaml
    resources:
      jobs:
        my_pipeline:
          name: my_pipeline
          tasks:
            - task_key: taskA
              package_name: my_package
            - task_key: taskB
              package_name: my_package
    ```

=== "After: resources/`[pipeline]`.yml"
    ```yaml hl_lines="10"
    resources:
      jobs:
        my_pipeline:
          name: my_pipeline
          tasks:
            - task_key: taskA
              package_name: my_package
            - task_key: taskB
              package_name: my_package
              run_if: AT_LEAST_ONE_SUCCESS
    ```
## individual_task

In this example, we tell `kedro-databricks` that we want to create two job clusters for the job: `default` and `high-performance`. We also specify that all tasks, except for the `high_performance_task`, should run on the `default` cluster. The `high_performance_task` will run on the `high-performance` cluster.

===! "conf/`[env]`/databricks.yml"
    ```yaml
    resources:
      jobs:
        default: # will be applied to all jobs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
            - job_cluster_key: high-performance
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 8
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          tasks: # will be applied to all tasks in each job
            - task_key: default
              job_cluster_key: default
            - task_key: make_predictions
              job_cluster_key: high-performance
    ```

=== "Before: resources/`[pipeline]`.yml"
    ```yaml
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```

=== "After: resources/`[pipeline]`.yml"
    ```yaml hl_lines="5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 22 38 55"
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
            - job_cluster_key: high-performance
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 8
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          tasks:
            - task_key: make_predictions
              job_cluster_key: high-performance
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              job_cluster_key: default
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              job_cluster_key: default
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```
## individual_task_with_parameters

In this example, we tell `kedro-databricks` that we want to create two job clusters for the job: `default` and `high-performance`. We also specify that all tasks, except for the `high_performance_task`, should run on the `default` cluster. The `high_performance_task` will run on the `high-performance` cluster.

In addition, we specify that the `high_performance_task` should run with a specific set of parameters, which are defined in the `parameters` section of the task definition.

===! "conf/`[env]`/databricks.yml"
    ```yaml
    resources:
      jobs:
        default: # will be applied to all jobs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
            - job_cluster_key: high-performance
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 8
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          tasks: # will be applied to all tasks in each job
            - task_key: default
              job_cluster_key: default
            - task_key: make_predictions
              job_cluster_key: high-performance
              python_wheel_task:
                parameters:
                  - --load-versions
                  - "dataset:2025-06-12"
    ```

=== "Before: resources/`[pipeline]`.yml"
    ```yaml
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```

=== "After: resources/`[pipeline]`.yml"
    ```yaml hl_lines="5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 22 35 36 40 57"
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
            - job_cluster_key: high-performance
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 8
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          tasks:
            - task_key: make_predictions
              job_cluster_key: high-performance
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
                  - --load-versions
                  - "dataset:2025-06-12"
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              job_cluster_key: default
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              job_cluster_key: default
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```
## individual_workflows

In this example, we tell `kedro-databricks` that we want to apply the overrides only to the job `my_job`.

===! "conf/`[env]`/databricks.yml"
    ```yaml
    resources:
      jobs:
        my_job:
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
    ```

=== "Before: resources/`[pipeline]`.yml"
    ```yaml
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
        my_job:
          name: my_job
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```

=== "After: resources/`[pipeline]`.yml"
    ```yaml hl_lines="52 53 54 55 56 57 58 59"
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
        my_job:
          name: my_job
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```
## merge_some_configs

In this example, we demonstrate the ability to merge a part of a configuration for a specific workflow. Related to issue [37](https://github.com/JenspederM/kedro-databricks/issues/37)

===! "conf/`[env]`/databricks.yml"
    ```yaml
    resources:
      jobs:
        default:
          # will be applied to all jobs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 15.3.x-scala2.12
                node_type_id: m4.large
                num_workers: 0
                runtime_engine: STANDARD
                data_security_mode: LEGACY_SINGLE_USER
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: "/dbfs/FileStore/david/conf/logging.yml"
                enable_elastic_disk: true
                spark_conf:
                  spark.databricks.cluster.profile: singleNode
                  spark.master: "local[*,4]"
                custom_tags:
                  ResourceClass: SingleNode
          tasks:
            # will be applied to all tasks in each job
            - task_key: default
              job_cluster_key: default
        develop_eggs:
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_env_vars:
                  PROVIDER_USERNAME: username
                  PROVIDER_PASSWORD: password
    ```

=== "Before: resources/`[pipeline]`.yml"
    ```yaml
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```

=== "After: resources/`[pipeline]`.yml"
    ```yaml hl_lines="5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 25 41 58"
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 15.3.x-scala2.12
                node_type_id: m4.large
                num_workers: 0
                runtime_engine: STANDARD
                data_security_mode: LEGACY_SINGLE_USER
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/david/conf/logging.yml
                  PROVIDER_USERNAME: username
                  PROVIDER_PASSWORD: password
                enable_elastic_disk: true
                spark_conf:
                  spark.databricks.cluster.profile: singleNode
                  spark.master: local[*,4]
                custom_tags:
                  ResourceClass: SingleNode
          tasks:
            - task_key: make_predictions
              job_cluster_key: default
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              job_cluster_key: default
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              job_cluster_key: default
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```
## regex_overrides

In this example, we tell `kedro-databricks` demonstrate how to use regexes to apply overrides to multiple jbos at once.

===! "conf/`[env]`/databricks.yml"
    ```yaml
    resources:
      jobs:
        "re:my_job.*":
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
    ```

=== "Before: resources/`[pipeline]`.yml"
    ```yaml
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
        my_job_1:
          name: my_job_1
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
        my_job_2:
          name: my_job_2
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```

=== "After: resources/`[pipeline]`.yml"
    ```yaml hl_lines="52 53 54 55 56 57 58 59 107 108 109 110 111 112 113 114"
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
        my_job_1:
          name: my_job_1
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
        my_job_2:
          name: my_job_2
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```
## task_level_webhook_notifications

In this example, we demonstrate how to use task-level webhook notifications in Kedro Databricks. This feature allows you to send notifications to a specified URL when a task starts or completes, which can be useful for monitoring and alerting purposes.

===! "conf/`[env]`/databricks.yml"
    ```yaml
    resources:
      jobs:
        default: # will be applied to all jobs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          tasks: # will be applied to all tasks in each job
            - task_key: default
              job_cluster_key: default
              webhook_notifications:
                on_start:
                  - id: on_start
                on_success:
                  - id: on_success
                on_failure:
                  - id: on_failure
                on_duration_warning_threshold_exceeded:
                  - id: on_duration_warning_threshold_exceeded
                on_streaming_backlog_exceeded:
                  - id: on_streaming_backlog_exceeded
    ```

=== "Before: resources/`[pipeline]`.yml"
    ```yaml
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```

=== "After: resources/`[pipeline]`.yml"
    ```yaml hl_lines="5 6 7 8 9 10 11 12 15 16 17 18 19 20 21 22 23 24 25 26 42 43 44 45 46 47 48 49 50 51 52 53 70 71 72 73 74 75 76 77 78 79 80 81"
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          tasks:
            - task_key: make_predictions
              job_cluster_key: default
              webhook_notifications:
                on_start:
                  - id: on_start
                on_success:
                  - id: on_success
                on_failure:
                  - id: on_failure
                on_duration_warning_threshold_exceeded:
                  - id: on_duration_warning_threshold_exceeded
                on_streaming_backlog_exceeded:
                  - id: on_streaming_backlog_exceeded
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              job_cluster_key: default
              webhook_notifications:
                on_start:
                  - id: on_start
                on_success:
                  - id: on_success
                on_failure:
                  - id: on_failure
                on_duration_warning_threshold_exceeded:
                  - id: on_duration_warning_threshold_exceeded
                on_streaming_backlog_exceeded:
                  - id: on_streaming_backlog_exceeded
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              job_cluster_key: default
              webhook_notifications:
                on_start:
                  - id: on_start
                on_success:
                  - id: on_success
                on_failure:
                  - id: on_failure
                on_duration_warning_threshold_exceeded:
                  - id: on_duration_warning_threshold_exceeded
                on_streaming_backlog_exceeded:
                  - id: on_streaming_backlog_exceeded
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```
## task_regex_overrides

In this example, we demonstrate how to apply overrides to multiple tasks using a regex identifier.

===! "conf/`[env]`/databricks.yml"
    ```yaml
    resources:
      jobs:
        default:
          # will be applied to all jobs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
            - job_cluster_key: high-performance
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 8
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          tasks:
            # will be applied to all tasks in each job
            - task_key: default
              job_cluster_key: default
            - task_key: "re:make_predictions.*"
              job_cluster_key: high-performance
    ```

=== "Before: resources/`[pipeline]`.yml"
    ```yaml
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: make_predictions_again
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```

=== "After: resources/`[pipeline]`.yml"
    ```yaml hl_lines="5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 22 38 54 71"
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
            - job_cluster_key: high-performance
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 8
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          tasks:
            - task_key: make_predictions
              job_cluster_key: high-performance
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: make_predictions_again
              job_cluster_key: high-performance
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              job_cluster_key: default
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              job_cluster_key: default
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```
## using_default_overrides

In this example, we demonstrate how to use default overrides in Kedro Databricks. Default overrides allow you to specify default configurations for your tasks and clusters, which can be applied across multiple jobs or tasks without needing to redefine them each time.

===! "conf/`[env]`/databricks.yml"
    ```yaml
    resources:
      jobs:
        default: # will be applied to all jobs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          tasks: # will be applied to all tasks in each job
            - task_key: default
              job_cluster_key: default
    ```

=== "Before: resources/`[pipeline]`.yml"
    ```yaml
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```

=== "After: resources/`[pipeline]`.yml"
    ```yaml hl_lines="5 6 7 8 9 10 11 12 15 31 48"
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          tasks:
            - task_key: make_predictions
              job_cluster_key: default
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              job_cluster_key: default
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              job_cluster_key: default
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```
## webhook_notifications

In this example, we demonstrate how to use webhook notifications in Kedro Databricks. This feature allows you to send notifications to a specified URL when a job starts or completes, which can be useful for monitoring and alerting purposes.

===! "conf/`[env]`/databricks.yml"
    ```yaml
    resources:
      jobs:
        default: # will be applied to all jobs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          webhook_notifications:
            on_start:
              - id: on_start
            on_success:
              - id: on_success
            on_failure:
              - id: on_failure
            on_duration_warning_threshold_exceeded:
              - id: on_duration_warning_threshold_exceeded
            on_streaming_backlog_exceeded:
              - id: on_streaming_backlog_exceeded
          tasks: # will be applied to all tasks in each job
            - task_key: default
              job_cluster_key: default
    ```

=== "Before: resources/`[pipeline]`.yml"
    ```yaml
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```

=== "After: resources/`[pipeline]`.yml"
    ```yaml hl_lines="5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 26 42 59"
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          webhook_notifications:
            on_start:
              - id: on_start
            on_success:
              - id: on_success
            on_failure:
              - id: on_failure
            on_duration_warning_threshold_exceeded:
              - id: on_duration_warning_threshold_exceeded
            on_streaming_backlog_exceeded:
              - id: on_streaming_backlog_exceeded
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          tasks:
            - task_key: make_predictions
              job_cluster_key: default
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              job_cluster_key: default
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              job_cluster_key: default
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```
## with_all_jobs_api_2.2_fields

In this example, we show all the available fields in the `kedro-databricks` configuration file for the Jobs API 2.2. This includes fields for job clusters, task clusters, and task-level configurations.
This example is useful for understanding the full range of configuration options available when using `kedro-databricks` with the Jobs API 2.2.

===! "conf/`[env]`/databricks.yml"
    ```yaml
    resources:
      jobs:
        default: # will be applied to all jobs
          description: "Default job cluster configuration"
          email_notifications:
            on_start:
              - on_start@email_notifications.com
            on_success:
              - on_success@email_notifications.com
            on_failure:
              - on_failure@email_notifications.com
            on_duration_warning_threshold_exceeded:
              - on_duration_warning_threshold_exceeded@email_notifications.com
            on_streaming_backlog_exceeded:
              - on_streaming_backlog_exceeded@email_notifications.com
            no_alert_for_skipped_runs: true
          webhook_notifications:
            on_start:
              - id: on_start@webhook_notifications.com
            on_success:
              - id: on_success@webhook_notifications.com
            on_failure:
              - id: on_failure@webhook_notifications.com
            on_duration_warning_threshold_exceeded:
              - id: on_duration_warning_threshold_exceeded@webhook_notifications.com
            on_streaming_backlog_exceeded:
              - id: on_streaming_backlog_exceeded@webhook_notifications.com
          notitication_settings:
            no_alert_for_skipped_runs: true
            no_alert_for_canceled_runs: true
          timeout_seconds: 3600
          health:
            rules:
              - metric: RUN_DURATION_SECONDS
                op: GREATER_THAN
                value: 10
          schedule:
            quartz_cron_expression: 0 0 0 ? * MON *
            timezone_id: America/New_York
            pause_status: PAUSED
          trigger:
            pause_status: PAUSED
            file_arrival:
              url: dbfs:/path/to/file
              min_time_between_triggers_seconds: 60
              wait_after_last_change_seconds: 60
            periodic:
              interval: 10
              unit: HOURS
          continuous:
            pause_status: PAUSED
          max_concurrent_runs: 10
          tasks: # will be applied to all tasks in each job
            - task_key: default
              job_cluster_key: default
              libraries:
                - whl: ../dist/*.whl
            - task_key: make_predictions
              depends_on:
                - task_key: report_accuracy
              run_if: ALL_SUCCESS
              notebook_task:
                notebook_path: /Users/username/notebooks/make_predictions
                base_parameters:
                  nodes: make_predictions
                  conf-source: /${workspace.file_path}/conf
                  env: local
                source: WORKSPACE
                warehouse_id: ab12cd34efgh567i
              spark_jar_task:
                main_class_name: com.databricks.ComputeModels
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              spark_python_task:
                python_file: /dbfs/FileStore/<package-name>/src/make_predictions.py
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
                source: GIT
              spark_submit_task:
                parameters:
                  - --jars my.jar
                  - --py-files my.py
                  - --files my.txt
              pipeline_task:
                pipeline_id: 1234-5678-9012-3456
                full_refresh: true
              python_wheel_task:
                package_name: my_package
                entry_point: my-entrypoint
                parameters:
                  - other-param
                named_parameters:
                  param1: value1
                  param2: value2
              dbt_task:
                project_directory: /dbfs/FileStore/<package-name>/dbt
                commands:
                  - --models
                  - my_model
                schema: default
                warehouse_id: ab12cd34efgh567i
                profiles_directory: /dbfs/FileStore/<package-name>/dbt/profiles
                source: WORKSPACE
                catalog: main
              sql_task:
                parameters:
                  age: 35
                  name: John Doe
                query: SELECT * FROM my_table WHERE name = ${name} AND age = ${age}
                dashboard:
                  dashboard_id: 1234-5678-9012-3456
                  subscriptions:
                    - user_name: John Doe
                      destination_id: 1234-5678-9012-3456
                  custom_subject: Custom subject
                  pause_subscriptions: true
                alert:
                  alert_id: 1234-5678-9012-3456
                  subscriptions:
                    - user_name: John Doe
                      destination_id: 1234-5678-9012-3456
                  pause_subscriptions: true
                file:
                  path: /dbfs/FileStore/<package-name>/sql/my_query.sql
                  source: WORKSPACE
                warehouse_id: ab12cd34efgh567i
              run_job_task:
                job_id: 1234-5678-9012-3456
                job_parameters:
                  param1: value1
                  param2: value2
                pipeline_parameters:
                  full_refresh: true
              conditional_task:
                op: EQUAL_TO
                left: ${task_name}
                right: make_predictions
              for_each_task:
                inputs:
                  - input1
                  - input2
                concurrency: 2
                task:
                  notebook_task:
                    notebook_path: /Users/username/notebooks/make_predictions
                    base_parameters:
                      nodes: make_predictions
                      conf-source: /${workspace.file_path}/conf
                      env: local
                    source: WORKSPACE
                    warehouse_id: ab12cd34efgh567i
              clean_room_notebook_task:
                clean_room_name: my_clean_room
                notebook_name: my_notebook
                etag: 1234-5678-9012-3456
                notebook_base_parameters:
                  param1: value1
                  param2: value2
              existing_cluster_id: 1234-5678-9012-3456
              job_cluster_key: my-key
              libraries:
                - whl: my-package.whl
              max_retries: 3
              min_retry_interval_millis: 2000
              retry_on_timeout: true
              disable_auto_optimization: false
              timeout_seconds: 3600
              health:
                rules:
                  - metric: RUN_DURATION_SECONDS
                    op: GREATER_THAN
                    value: 10
              email_notifications:
                on_start:
                  - on_start@email_notifications.com
                on_success:
                  - on_success@email_notifications.com
                on_failure:
                  - on_failure@email_notifications.com
                on_duration_warning_threshold_exceeded:
                  - on_duration_warning_threshold_exceeded@email_notifications.com
                on_streaming_backlog_exceeded:
                  - on_streaming_backlog_exceeded@email_notifications.com
                no_alert_for_skipped_runs: true
              webhook_notifications:
                on_start:
                  - id: on_start@webhook_notifications.com
                on_success:
                  - id: on_success@webhook_notifications.com
                on_failure:
                  - id: on_failure@webhook_notifications.com
                on_duration_warning_threshold_exceeded:
                  - id: on_duration_warning_threshold_exceeded@webhook_notifications.com
                on_streaming_backlog_exceeded:
                  - id: on_streaming_backlog_exceeded@webhook_notifications.com
              description: "Default task configuration"
              environment_key: default
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                num_workers: 2
                autoscale:
                  min_workers: 2
                  max_workers: 3
                kind: CLASSIC_PREVIEW
                cluster_name: my-cluster
                spark_version: 7.3.x-scala2.12
                use_ml_runtime: true
                is_single_node: false
                spark_conf:
                  spark.executor.memory: 2g
                  spark.executor.cores: 2
                  spark.driver.memory: 2g
                  spark.driver.cores: 2
                aws_attributes:
                  first_on_demand: 1
                  availability: SPOT_WITH_FALLBACK
                  zone_id: us-west-2a
                  instance_profile_arn: arn:aws:iam::123456789012:instance-profile/my-instance-profile
                  spot_bid_price_percent: 100
                  ebs_volume_type: gp2
                  ebs_volume_count: 1
                  ebs_volume_size: 100
                  ebs_volume_iops: 400
                  ebs_volume_throughput: 400
                node_type_id: Standard_DS3_v2
                driver_node_type_id: Standard_DS3_v2
                ssh_public_keys:
                  - ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDf6
                custom_tags:
                  my_tag: my_value
                cluster_log_conf:
                  dbfs:
                    destination: dbfs:/cluster-logs
                  s3:
                    destination: s3://my-bucket/cluster-logs
                    region: us-west-2
                    endpoint: s3.amazonaws.com
                    enable_encryption: true
                    encryption_type: SSE-KMS
                    kms_key: arn:aws:kms:us-west-2:123456789012:key/1234-5678-9012-3456
                    canned_acl: private
                  volumes:
                    destination: /mnt/volume
                init_scripts:
                  - workspace:
                      destination: /dbfs/FileStore/<package-name>/init_scripts
                    volumes:
                      destination: /mnt/volume
                    s3:
                      destination: s3://my-bucket/cluster-logs
                      region: us-west-2
                      endpoint: s3.amazonaws.com
                      enable_encryption: true
                      encryption_type: SSE-KMS
                      kms_key: arn:aws:kms:us-west-2:123456789012:key/1234-5678-9012-3456
                      canned_acl: private
                    file:
                      destination: file://dbfs/FileStore/<package-name>/init_scripts
                    dbfs:
                      destination: dbfs:/cluster-logs
                    abfss:
                      destination: abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<directory-name>
                    gcs:
                      destination: gs://my-bucket/cluster-logs
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
                autotermination_minutes: 60
                enable_elastic_disk: true
                instance_pool_id: 1234-5678-9012-3456
                policy_id: 1234-5678-9012-3456
                enable_local_disk_encryption: true
                driver_instance_pool_id: 1234-5678-9012-3456
                workload_type:
                  clients:
                    notebooks: true
                    jobs: true
                runtime_engine: PHOTON
                docker_image:
                  url: databricksruntime/standard:latest
                  basic_auth:
                    username: my_username
                    password: my_password
                data_security_mode: DATA_SECURITY_MODE_AUTO
                single_user_name: my_username
                apply_policy_default_values: true
          git_source:
            git_url: https://github.com/databricks/databricks-cli
            git_provider: GITHUB_ENTERPRISE
            git_branch: master
            git_tag: v1.0.0
            git_commit: 1234567890abcdef
            git_snapshot:
              used_commit: 4506fdf41e9fa98090570a34df7a5bce163ff15f
          tags:
            cost-center: 1234
            owner: John Doe
          format: MULTI_TASK
          queue:
            enabled: true
          parameters:
            - name: param1
              default: value1
            - name: param2
              default: value2
          run_as:
            user_name: my_username
            service_principal_name: my_service_principal_name
          edit_mode: UI_LOCKED
          deployment:
            kind: bundle
            metadata_file_path: /dbfs/FileStore/<package-name>/metadata.json
          environments:
            - environment_key: my-environment
              spec:
                client: 1
                dependencies:
                  - ../dist/*.whl
          budget_policy_id: 1234-5678-9012-3456
          access_control_list:
            - group_name: my_group
              permission_level: CAN_RUN
            - user_name: my_user
              permission_level: CAN_MANAGE
            - service_principal_name: my_service_principal
              permission_level: CAN_VIEW
    ```

=== "Before: resources/`[pipeline]`.yml"
    ```yaml
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          description: This is my resource
          email_notifications:
            on_start:
              - resource@email_notifications.com
          notitication_settings:
            no_alert_for_skipped_runs: false
          webhook_notifications:
            on_start:
              - id: resource@webhook_notifications.com
          health:
            rules:
              - metric: RUN_DURATION_SECONDS
                op: GREATER_THAN
                value: 5
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```

=== "After: resources/`[pipeline]`.yml"
    ```yaml hl_lines="5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119 120 121 122 123 124 125 126 127 128 129 130 131 132 133 134 135 136 137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 153 154 155 156 157 158 159 160 161 162 163 164 165 166 167 168 169 170 171 172 173 174 175 176 177 178 179 180 181 182 183 184 185 186 187 188 189 190 191 192 193 194 195 196 197 198 199 200 201 202 203 204 205 206 207 208 209 210 211 212 213 214 215 216 217 218 219 220 221 222 223 224 225 226 227 228 229 230 231 232 233 234 235 236 237 238 239 240 241 242 243 244 245 246 247 248 249 250 251 252 253 254 255 256 257 258 259 260 261 262 263 264 265 266 267 268 269 270 271 272 273 274 275 276 277 278 279 280 281 282 283 284 285 286 287 288 289 290 291 292 293 294 295 296 297 298 299 300 301 302 303 304 305 306 307 308 309 310 311 312 313 314 315 316 317 318 319 320 321 322 323 324 325 326 327 328 329 330 331 332 333 334 335 336 337 338 339 340 341 342 343 344 345 346 347 348 349 350 351 352 353 354 355 356 357 358 359 360 361 362 363 364 365 366 367 368 369 370 371 372 373 374"
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          description: "Default job cluster configuration"
          email_notifications:
            on_start:
              - resource@email_notifications.com
              - on_start@email_notifications.com
            on_success:
              - on_success@email_notifications.com
            on_failure:
              - on_failure@email_notifications.com
            on_duration_warning_threshold_exceeded:
              - on_duration_warning_threshold_exceeded@email_notifications.com
            on_streaming_backlog_exceeded:
              - on_streaming_backlog_exceeded@email_notifications.com
            no_alert_for_skipped_runs: true
          webhook_notifications:
            on_start:
              - id: on_start@webhook_notifications.com
              - id: resource@webhook_notifications.com
            on_success:
              - id: on_success@webhook_notifications.com
            on_failure:
              - id: on_failure@webhook_notifications.com
            on_duration_warning_threshold_exceeded:
              - id: on_duration_warning_threshold_exceeded@webhook_notifications.com
            on_streaming_backlog_exceeded:
              - id: on_streaming_backlog_exceeded@webhook_notifications.com
          notitication_settings:
            no_alert_for_skipped_runs: true
            no_alert_for_canceled_runs: true
          timeout_seconds: 3600
          health:
            rules:
              - metric: RUN_DURATION_SECONDS
                op: GREATER_THAN
                value: 10
          schedule:
            quartz_cron_expression: 0 0 0 ? * MON *
            timezone_id: America/New_York
            pause_status: PAUSED
          trigger:
            pause_status: PAUSED
            file_arrival:
              url: dbfs:/path/to/file
              min_time_between_triggers_seconds: 60
              wait_after_last_change_seconds: 60
            periodic:
              interval: 10
              unit: HOURS
          continuous:
            pause_status: PAUSED
          max_concurrent_runs: 10
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: report_accuracy
                - task_key: split
              run_if: ALL_SUCCESS
              notebook_task:
                notebook_path: /Users/username/notebooks/make_predictions
                base_parameters:
                  nodes: make_predictions
                  conf-source: /${workspace.file_path}/conf
                  env: local
                source: WORKSPACE
                warehouse_id: ab12cd34efgh567i
              spark_jar_task:
                main_class_name: com.databricks.ComputeModels
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              spark_python_task:
                python_file: /dbfs/FileStore/<package-name>/src/make_predictions.py
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
                source: GIT
              spark_submit_task:
                parameters:
                  - --jars my.jar
                  - --py-files my.py
                  - --files my.txt
              pipeline_task:
                pipeline_id: 1234-5678-9012-3456
                full_refresh: true
              python_wheel_task:
                package_name: my_package
                entry_point: my-entrypoint
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
                  - other-param
                named_parameters:
                  param1: value1
                  param2: value2
              dbt_task:
                project_directory: /dbfs/FileStore/<package-name>/dbt
                commands:
                  - --models
                  - my_model
                schema: default
                warehouse_id: ab12cd34efgh567i
                profiles_directory: /dbfs/FileStore/<package-name>/dbt/profiles
                source: WORKSPACE
                catalog: main
              sql_task:
                parameters:
                  age: 35
                  name: John Doe
                query: SELECT * FROM my_table WHERE name = ${name} AND age = ${age}
                dashboard:
                  dashboard_id: 1234-5678-9012-3456
                  subscriptions:
                    - user_name: John Doe
                      destination_id: 1234-5678-9012-3456
                  custom_subject: Custom subject
                  pause_subscriptions: true
                alert:
                  alert_id: 1234-5678-9012-3456
                  subscriptions:
                    - user_name: John Doe
                      destination_id: 1234-5678-9012-3456
                  pause_subscriptions: true
                file:
                  path: /dbfs/FileStore/<package-name>/sql/my_query.sql
                  source: WORKSPACE
                warehouse_id: ab12cd34efgh567i
              run_job_task:
                job_id: 1234-5678-9012-3456
                job_parameters:
                  param1: value1
                  param2: value2
                pipeline_parameters:
                  full_refresh: true
              conditional_task:
                op: EQUAL_TO
                left: ${task_name}
                right: make_predictions
              for_each_task:
                inputs:
                  - input1
                  - input2
                concurrency: 2
                task:
                  notebook_task:
                    notebook_path: /Users/username/notebooks/make_predictions
                    base_parameters:
                      nodes: make_predictions
                      conf-source: /${workspace.file_path}/conf
                      env: local
                    source: WORKSPACE
                    warehouse_id: ab12cd34efgh567i
              clean_room_notebook_task:
                clean_room_name: my_clean_room
                notebook_name: my_notebook
                etag: 1234-5678-9012-3456
                notebook_base_parameters:
                  param1: value1
                  param2: value2
              existing_cluster_id: 1234-5678-9012-3456
              job_cluster_key: my-key
              libraries:
                - whl: ../dist/*.whl
                - whl: my-package.whl
              max_retries: 3
              min_retry_interval_millis: 2000
              retry_on_timeout: true
              disable_auto_optimization: false
              timeout_seconds: 3600
              health:
                rules:
                  - metric: RUN_DURATION_SECONDS
                    op: GREATER_THAN
                    value: 10
              email_notifications:
                on_start:
                  - on_start@email_notifications.com
                on_success:
                  - on_success@email_notifications.com
                on_failure:
                  - on_failure@email_notifications.com
                on_duration_warning_threshold_exceeded:
                  - on_duration_warning_threshold_exceeded@email_notifications.com
                on_streaming_backlog_exceeded:
                  - on_streaming_backlog_exceeded@email_notifications.com
                no_alert_for_skipped_runs: true
              webhook_notifications:
                on_start:
                  - id: on_start@webhook_notifications.com
                on_success:
                  - id: on_success@webhook_notifications.com
                on_failure:
                  - id: on_failure@webhook_notifications.com
                on_duration_warning_threshold_exceeded:
                  - id: on_duration_warning_threshold_exceeded@webhook_notifications.com
                on_streaming_backlog_exceeded:
                  - id: on_streaming_backlog_exceeded@webhook_notifications.com
              description: "Default task configuration"
              environment_key: default
            - task_key: report_accuracy
              job_cluster_key: default
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              job_cluster_key: default
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                num_workers: 2
                autoscale:
                  min_workers: 2
                  max_workers: 3
                kind: CLASSIC_PREVIEW
                cluster_name: my-cluster
                spark_version: 7.3.x-scala2.12
                use_ml_runtime: true
                is_single_node: false
                spark_conf:
                  spark.executor.memory: 2g
                  spark.executor.cores: 2
                  spark.driver.memory: 2g
                  spark.driver.cores: 2
                aws_attributes:
                  first_on_demand: 1
                  availability: SPOT_WITH_FALLBACK
                  zone_id: us-west-2a
                  instance_profile_arn: arn:aws:iam::123456789012:instance-profile/my-instance-profile
                  spot_bid_price_percent: 100
                  ebs_volume_type: gp2
                  ebs_volume_count: 1
                  ebs_volume_size: 100
                  ebs_volume_iops: 400
                  ebs_volume_throughput: 400
                node_type_id: Standard_DS3_v2
                driver_node_type_id: Standard_DS3_v2
                ssh_public_keys:
                  - ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDf6
                custom_tags:
                  my_tag: my_value
                cluster_log_conf:
                  dbfs:
                    destination: dbfs:/cluster-logs
                  s3:
                    destination: s3://my-bucket/cluster-logs
                    region: us-west-2
                    endpoint: s3.amazonaws.com
                    enable_encryption: true
                    encryption_type: SSE-KMS
                    kms_key: arn:aws:kms:us-west-2:123456789012:key/1234-5678-9012-3456
                    canned_acl: private
                  volumes:
                    destination: /mnt/volume
                init_scripts:
                  - workspace:
                      destination: /dbfs/FileStore/<package-name>/init_scripts
                    volumes:
                      destination: /mnt/volume
                    s3:
                      destination: s3://my-bucket/cluster-logs
                      region: us-west-2
                      endpoint: s3.amazonaws.com
                      enable_encryption: true
                      encryption_type: SSE-KMS
                      kms_key: arn:aws:kms:us-west-2:123456789012:key/1234-5678-9012-3456
                      canned_acl: private
                    file:
                      destination: file://dbfs/FileStore/<package-name>/init_scripts
                    dbfs:
                      destination: dbfs:/cluster-logs
                    abfss:
                      destination: abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<directory-name>
                    gcs:
                      destination: gs://my-bucket/cluster-logs
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
                autotermination_minutes: 60
                enable_elastic_disk: true
                instance_pool_id: 1234-5678-9012-3456
                policy_id: 1234-5678-9012-3456
                enable_local_disk_encryption: true
                driver_instance_pool_id: 1234-5678-9012-3456
                workload_type:
                  clients:
                    notebooks: true
                    jobs: true
                runtime_engine: PHOTON
                docker_image:
                  url: databricksruntime/standard:latest
                  basic_auth:
                    username: my_username
                    password: my_password
                data_security_mode: DATA_SECURITY_MODE_AUTO
                single_user_name: my_username
                apply_policy_default_values: true
          git_source:
            git_url: https://github.com/databricks/databricks-cli
            git_provider: GITHUB_ENTERPRISE
            git_branch: master
            git_tag: v1.0.0
            git_commit: 1234567890abcdef
            git_snapshot:
              used_commit: 4506fdf41e9fa98090570a34df7a5bce163ff15f
          tags:
            cost-center: 1234
            owner: John Doe
          format: MULTI_TASK
          queue:
            enabled: true
          parameters:
            - name: param1
              default: value1
            - name: param2
              default: value2
          run_as:
            user_name: my_username
            service_principal_name: my_service_principal_name
          edit_mode: UI_LOCKED
          deployment:
            kind: bundle
            metadata_file_path: /dbfs/FileStore/<package-name>/metadata.json
          environments:
            - environment_key: my-environment
              spec:
                client: 1
                dependencies:
                  - ../dist/*.whl
          budget_policy_id: 1234-5678-9012-3456
          access_control_list:
            - group_name: my_group
              permission_level: CAN_RUN
            - user_name: my_user
              permission_level: CAN_MANAGE
            - service_principal_name: my_service_principal
              permission_level: CAN_VIEW
    ```
## with_custom_libraries

In this example, we demonstrate how to use custom libraries in Kedro Databricks. This feature allows you to specify additional libraries that should be installed in the Databricks environment when running your Kedro project. This is useful for ensuring that all necessary dependencies are available for your jobs.

===! "conf/`[env]`/databricks.yml"
    ```yaml
    resources:
      jobs:
        default:
          webhook_notifications:
            on_failure:
              - id: NOTIFICATION_SOURCE_ID
          tasks:
            - task_key: default # will be applied to all tasks in the specified job
              existing_cluster_id: CLUSTER_ID
              libraries:
                - whl: /Workspace/packages/internal-package-0.1.1-py3-none-any.whl
                - whl: ../dist/*.whl
    ```

=== "Before: resources/`[pipeline]`.yml"
    ```yaml
    resources:
      jobs:
        my_pipeline:
          name: my_pipeline
          tasks:
            - task_key: taskA
              package_name: my_package
            - task_key: taskB
              package_name: my_package
    ```

=== "After: resources/`[pipeline]`.yml"
    ```yaml hl_lines="5 6 7 11 12 13 14 17 18 19 20"
    resources:
      jobs:
        my_pipeline:
          name: my_pipeline
          webhook_notifications:
            on_failure:
              - id: NOTIFICATION_SOURCE_ID
          tasks:
            - task_key: taskA
              package_name: my_package
              existing_cluster_id: CLUSTER_ID
              libraries: # whls are sorted alphabetically
                - whl: ../dist/*.whl
                - whl: /Workspace/packages/internal-package-0.1.1-py3-none-any.whl
            - task_key: taskB
              package_name: my_package
              existing_cluster_id: CLUSTER_ID
              libraries: # whls are sorted alphabetically
                - whl: ../dist/*.whl
                - whl: /Workspace/packages/internal-package-0.1.1-py3-none-any.whl
    ```
## with_health_rules

In this example, we demonstrate how to use health rules in Kedro Databricks. Health rules allow you to define conditions that must be met for a job to be considered healthy. If a job fails to meet these conditions, it can trigger alerts or notifications.

===! "conf/`[env]`/databricks.yml"
    ```yaml
    resources:
      jobs:
        default: # will be applied to all jobs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          health:
            rules:
              - metric: RUN_DURATION_SECONDS
                op: GREATER_THAN
                value: 10
          tasks: # will be applied to all tasks in each job
            - task_key: default
              job_cluster_key: default
    ```

=== "Before: resources/`[pipeline]`.yml"
    ```yaml
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```

=== "After: resources/`[pipeline]`.yml"
    ```yaml hl_lines="5 6 7 8 9 10 11 12 13 14 15 16 17 20 36 53"
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          health:
            rules:
              - metric: RUN_DURATION_SECONDS
                op: GREATER_THAN
                value: 10
          tasks:
            - task_key: make_predictions
              job_cluster_key: default
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              job_cluster_key: default
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              job_cluster_key: default
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```
## with_job_parameters

In this example, we demonstrate how to set job-level parameters in Kedro Databricks. Job-level parameters allow you to define parameters that can be used across all tasks in a job, providing a way to configure the job's behavior without modifying individual task definitions.

===! "conf/`[env]`/databricks.yml"
    ```yaml
    resources:
      jobs:
        default: # will be applied to all jobs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          parameters:
            - name: my_param
              default: some value
          tasks: # will be applied to all tasks in each job
            - task_key: default
              job_cluster_key: default
    ```

=== "Before: resources/`[pipeline]`.yml"
    ```yaml
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          tasks:
            - task_key: make_predictions
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```

=== "After: resources/`[pipeline]`.yml"
    ```yaml hl_lines="5 6 7 8 9 10 11 12 13 14 15 18 34 51"
    resources:
      jobs:
        develop_eggs:
          name: develop_eggs
          job_clusters:
            - job_cluster_key: default
              new_cluster:
                spark_version: 7.3.x-scala2.12
                node_type_id: Standard_DS3_v2
                num_workers: 2
                spark_env_vars:
                  KEDRO_LOGGING_CONFIG: /dbfs/FileStore/<package-name>/conf/logging.yml
          parameters:
            - name: my_param
              default: some value
          tasks:
            - task_key: make_predictions
              job_cluster_key: default
              depends_on:
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - make_predictions
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: report_accuracy
              job_cluster_key: default
              depends_on:
                - task_key: make_predictions
                - task_key: split
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - report_accuracy
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
            - task_key: split
              job_cluster_key: default
              python_wheel_task:
                package_name: develop_eggs
                entry_point: develop-eggs
                parameters:
                  - --nodes
                  - split
                  - --conf-source
                  - /${workspace.file_path}/conf
                  - --env
                  - local
              libraries:
                - whl: ../dist/*.whl
    ```
