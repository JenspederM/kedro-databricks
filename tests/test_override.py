from copy import copy
import pytest
from kedro_databricks.bundle import _validate, apply_resource_overrides


bad_conf = {
    "default": {
        "job_clusters": [
            {
                "job_cluster_key": "default",
                "new_cluster": {
                    "spark_version": "14.3.x-scala2.12",
                    "node_type_id": "Standard_D4ds_v4",
                    "num_workers": 1,
                },
            }
        ],
        "tasks": [{"task_key": "default", "job_cluster_key": "default"}],
    }
}

bad_resource = {
    "resources": {
        "jobs": {
            "dev_project": {
                "name": "dev_project",
                "job_clusters": [{"job_cluster_key": "default"}],
                "tasks": [
                    {
                        "task_key": "hello_world_node",
                        "job_cluster_key": "default",
                        "depends_on": [],
                        "python_wheel_task": {
                            "package_name": "dev_project",
                            "entry_point": "databricks_run",
                            "parameters": [
                                "--nodes",
                                "hello_world_node",
                                "--conf-source",
                                "/dbfs/FileStore/dev_project/conf",
                                "--package-name",
                                "dev_project",
                            ],
                        },
                        "libraries": [{"whl": "../dist/*.whl"}],
                    }
                ],
                "format": "MULTI_TASK",
            }
        }
    }
}

good_conf = {
    "default": {
        "job_clusters": [
            {
                "job_cluster_key": "default",
                "new_cluster": {
                    "spark_version": "14.3.x-scala2.12",
                    "node_type_id": "Standard_D4ds_v4",
                    "num_workers": 1,
                    "spark_env_vars": {
                        "KEDRO_LOGGING_CONFIG": "/dbfs/FileStore/dev_project/conf/logging.yml"
                    },
                },
            }
        ],
        "tasks": [{"task_key": "default", "job_cluster_key": "default"}],
    }
}

good_resource = {
    "resources": {
        "jobs": {
            "dev_project": {
                "name": "dev_project",
                "job_clusters": [
                    {
                        "job_cluster_key": "default",
                        "new_cluster": {
                            "spark_version": "14.3.x-scala2.12",
                            "node_type_id": "Standard_D4ds_v4",
                            "num_workers": 1,
                            "spark_env_vars": {
                                "KEDRO_LOGGING_CONFIG": "/dbfs/FileStore/dev_project/conf/logging.yml"
                            },
                        },
                    }
                ],
                "tasks": [
                    {
                        "task_key": "hello_world_node",
                        "job_cluster_key": "default",
                        "depends_on": [],
                        "python_wheel_task": {
                            "package_name": "dev_project",
                            "entry_point": "databricks_run",
                            "parameters": [
                                "--nodes",
                                "hello_world_node",
                                "--conf-source",
                                "/dbfs/FileStore/dev_project/conf",
                                "--package-name",
                                "dev_project",
                            ],
                        },
                        "libraries": [{"whl": "../dist/*.whl"}],
                    }
                ],
                "format": "MULTI_TASK",
            }
        }
    }
}


def test__validate():
    assert _validate(bad_conf.get("default")) == [
        'KEDRO_LOGGING_CONFIG not found in spark_env_vars for cluster "default"'
    ]
    assert _validate(good_conf.get("default")) == []


def test_apply_resource_overrides():
    with pytest.raises(ValueError):
        apply_resource_overrides(bad_resource, bad_conf, "default", "dev_project")

    wf = copy(good_resource)
    wf["resources"]["jobs"]["dev_project"]["job_clusters"][0].pop("new_cluster")

    new_resource = apply_resource_overrides(
        {"dev_project": wf},
        good_conf,
        "default",
        "dev_project",
    )

    assert new_resource["dev_project"] == good_resource
