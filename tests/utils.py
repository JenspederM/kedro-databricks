def _reset_init(metadata):
    (metadata.project_path / "databricks.yml").unlink(missing_ok=True)
    (metadata.project_path / "conf" / "base" / "databricks.yml").unlink(missing_ok=True)
