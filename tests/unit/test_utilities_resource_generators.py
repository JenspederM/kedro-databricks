import pytest
from kedro.framework.session import KedroSession
from kedro.pipeline import Pipeline

from kedro_databricks.config import config
from kedro_databricks.utilities.common import require_databricks_run_script
from kedro_databricks.utilities.resource_generator import (
    NodeResourceGenerator,
    PipelineResourceGenerator,
)
from tests.utils import (
    JOB,
    _generate_task,
    create_catalog,
    identity,
    long_identity,
    node,
    pipeline,
)


def test_create_job(metadata):
    create_catalog(metadata, config.default_env)
    with KedroSession.create(
        project_path=metadata.project_path, env=config.default_env
    ) as session:
        g = NodeResourceGenerator(session=session, metadata=metadata)
        assert g._create_job("job1", pipeline, "__default__") == JOB


def test_create_job_pipeline(metadata):
    create_catalog(metadata, config.default_env)
    with KedroSession.create(
        project_path=metadata.project_path, env=config.default_env
    ) as session:
        g = PipelineResourceGenerator(session=session, metadata=metadata)
        assert g._create_job("job1", pipeline, "__default__") is not None


def test_create_job_pipeline_fails(metadata):
    (metadata.project_path / "conf" / config.default_env).mkdir(
        parents=True, exist_ok=True
    )
    with KedroSession.create(
        project_path=metadata.project_path, env=config.default_env
    ) as session:
        g = PipelineResourceGenerator(session=session, metadata=metadata)
        with pytest.raises(
            ValueError,
            match="Pipeline name must be provided to create a job dict when --pipeline is used.",
        ):
            g._create_job("job1", pipeline, None)  # type: ignore


def test_create_task(metadata):
    create_catalog(metadata, config.default_env)
    with KedroSession.create(
        project_path=metadata.project_path, env=config.default_env
    ) as session:
        g = NodeResourceGenerator(session=session, metadata=metadata)
        expected_task = _generate_task("task", ["a", "b"])
        node_a = node(identity, ["input"], ["output"], name="a")
        node_b = node(identity, ["input"], ["output"], name="b")
        assert (
            g._create_task(
                node(
                    long_identity,
                    ["input", "input1", "input2"],
                    ["output", "output1"],
                    name="task",
                ),
                [
                    node_b,
                    node_a,
                ],
            )
            == expected_task
        )


def test_create_pipeline_task(metadata):
    create_catalog(metadata, config.default_env)
    with KedroSession.create(
        project_path=metadata.project_path, env=config.default_env
    ) as session:
        g = PipelineResourceGenerator(session=session, metadata=metadata)
        pipeline_name = "pipeline_task"

        if require_databricks_run_script():
            entry_point = "databricks_run"
            extra_params = ["--package-name", "fake_project"]
        else:
            entry_point = "fake-project"
            extra_params = []

        assert g._create_pipeline_task(pipeline_name) == {
            "task_key": pipeline_name,
            "depends_on": [],
            "python_wheel_task": {
                "entry_point": entry_point,
                "package_name": "fake_project",
                "parameters": [
                    "--pipeline",
                    pipeline_name,
                    "--conf-source",
                    "/${workspace.file_path}/conf",
                    "--env",
                    "${var.environment}",
                    *extra_params,
                ],
            },
        }


def test_create_task_with_runtime_params(metadata):
    create_catalog(metadata, config.default_env)
    with KedroSession.create(
        project_path=metadata.project_path, env=config.default_env
    ) as session:
        g = NodeResourceGenerator(
            session=session, metadata=metadata, params="key1=value1,key2=value2"
        )
        expected_task = _generate_task(
            "task", ["a", "b"], runtime_params="key1=value1,key2=value2"
        )
        node_a = node(identity, ["input"], ["output"], name="a")
        node_b = node(identity, ["input"], ["output"], name="b")
        assert (
            g._create_task(
                node(
                    long_identity,
                    ["input", "input1", "input2"],
                    ["output", "output1"],
                    name="task",
                ),
                [
                    node_b,
                    node_a,
                ],
            )
            == expected_task
        )


def test_generate_resources(metadata):
    create_catalog(metadata, config.default_env)
    with KedroSession.create(
        project_path=metadata.project_path, env=config.default_env
    ) as session:
        g = NodeResourceGenerator(session=session, metadata=metadata)
        g.pipelines = {"__default__": Pipeline([])}
        assert g.generate_jobs(pipeline_name=None) == {}
        g.pipelines = {
            "__default__": Pipeline(
                [node(identity, ["input"], ["output"], name="node")]
            )
        }
        assert g.generate_jobs(pipeline_name=None) == {
            "fake_project": {
                "name": "fake_project",
                "tasks": [
                    _generate_task("node"),
                ],
            },
        }


def test_generate_resources_non_existent_pipeline(metadata):
    create_catalog(metadata, config.default_env)
    with KedroSession.create(
        project_path=metadata.project_path, env=config.default_env
    ) as session:
        g = NodeResourceGenerator(session=session, metadata=metadata)
        g.pipelines = {"__default__": Pipeline([])}
        assert g.generate_jobs(pipeline_name=None) == {}
        g.pipelines = {
            "__default__": Pipeline(
                [node(identity, ["input"], ["output"], name="node")]
            )
        }
        with pytest.raises(
            KeyError,
            match="Pipeline 'non_existent_pipeline' not found. Available pipelines:",
        ):
            g.generate_jobs(pipeline_name="non_existent_pipeline")


def test_generate_resources_another_conf(metadata):
    create_catalog(metadata, config.default_env)
    with KedroSession.create(
        project_path=metadata.project_path, env=config.default_env
    ) as session:
        g = NodeResourceGenerator(
            session=session, metadata=metadata, conf_source="sub_conf"
        )
        g.pipelines = {
            "__default__": Pipeline(
                [node(identity, ["input"], ["output"], name="node")]
            )
        }
        assert g.generate_jobs(pipeline_name=None) == {
            "fake_project": {
                "name": "fake_project",
                "tasks": [
                    _generate_task("node", conf="sub_conf"),
                ],
            },
        }


def test_generate_resources_in_a_sorted_manner(metadata):
    create_catalog(metadata, config.default_env)
    with KedroSession.create(
        project_path=metadata.project_path, env=config.default_env
    ) as session:
        g = NodeResourceGenerator(session=session, metadata=metadata)
        g.pipelines = {
            "__default__": Pipeline(
                [
                    node(identity, ["input"], ["b_output"], name="b_node"),
                    node(identity, ["input"], ["a_output"], name="a_node"),
                ]
            )
        }
        assert g.generate_jobs(pipeline_name=None) == {
            "fake_project": {
                "name": "fake_project",
                "tasks": [
                    _generate_task("a_node"),
                    _generate_task("b_node"),
                ],
            },
        }


def test_generate_resources_for_a_single_pipeline(metadata):
    create_catalog(metadata, config.default_env)
    with KedroSession.create(
        project_path=metadata.project_path, env=config.default_env
    ) as session:
        g = NodeResourceGenerator(session=session, metadata=metadata)
        g.pipelines = {
            "__default__": Pipeline(
                [
                    node(identity, ["input"], ["a_output"], name="a_node"),
                ]
            ),
            "a_pipeline": Pipeline(
                [
                    node(identity, ["input"], ["a_output"], name="a_node"),
                ]
            ),
            "b_pipeline": Pipeline(
                [
                    node(identity, ["input"], ["b_output"], name="b_node"),
                ]
            ),
        }
        assert g.generate_jobs(pipeline_name="b_pipeline") == {
            "fake_project_b_pipeline": {
                "name": "fake_project_b_pipeline",
                "tasks": [
                    _generate_task("b_node"),
                ],
            },
        }
