from kedro.framework.startup import ProjectMetadata


def reset_init(metadata: ProjectMetadata):
    (metadata.project_path / "databricks.yml").unlink(missing_ok=True)
    (metadata.project_path / "conf" / "base" / "databricks.yml").unlink(missing_ok=True)
