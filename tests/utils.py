import shutil

from kedro.framework.startup import ProjectMetadata


def reset_init(metadata: ProjectMetadata):
    (metadata.project_path / "databricks.yml").unlink(missing_ok=True)
    shutil.rmtree(metadata.project_path / "conf" / "dev", ignore_errors=True)
    shutil.rmtree(metadata.project_path / "conf" / "prod", ignore_errors=True)
