import subprocess
import tempfile
import json
import shutil


def deploy_to_databricks(package_name: str):
    if shutil.which("databricks") is None:
        raise Exception("databricks CLI is not installed")

    config = {"project_name": package_name, "project_slug": package_name}

    f = tempfile.NamedTemporaryFile(delete=False)
    f.write(json.dumps(config).encode())
    f.close()

    subprocess.call(
        [
            "databricks",
            "bundle",
            "init",
            "assets/template",
            "--config-file",
            f.name,
        ]
    )

    f.unlink()
    pass


if __name__ == "__main__":
    deploy_to_databricks()
