In this example, we tell `kedro-databricks` that we want to create two job clusters for the workflow: `default` and `high-performance`. We also specify that all tasks, except for the `high_performance_task`, should run on the `default` cluster. The `high_performance_task` will run on the `high-performance` cluster.

In addition, we specify that the `high_performance_task` should run with a specific set of parameters, which are defined in the `parameters` section of the task definition.
