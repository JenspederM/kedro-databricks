from kedro.framework.startup import ProjectMetadata

from kedro_databricks.logger import get_logger
from kedro_databricks.utils import Command, assert_databricks_cli

log = get_logger("run")


def run(metadata: ProjectMetadata, pipeline: str, *databricks_args):
    assert_databricks_cli()
    cmd = ["databricks", "bundle", "run", pipeline] + list(databricks_args)
    log.info(f"Running `{' '.join(cmd)}` in {metadata.project_path}")
    result = Command(cmd, log=log, warn=True).run(cwd=metadata.project_path)
    if result.returncode != 0:  # pragma: no cover
        raise RuntimeError("Failed to run Databricks job\n" + "\n".join(result.stdout))
