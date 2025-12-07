import argparse
import logging
import shutil
import subprocess
from importlib.metadata import version
from pathlib import Path

log = logging.getLogger("kedro_databricks")


def new_project(name):
    """Create a new Kedro project using the Databricks starter."""
    subprocess.run(
        ["kedro", "new", "--starter", "databricks-iris", "--name", name], check=True
    )


def run_kedro_databricks_flow(name, destroy=False):
    ver = version("kedro_databricks")
    whl = f"kedro_databricks-{ver}-py3-none-any.whl"
    logging.basicConfig(level=logging.INFO)
    subprocess.run(["uv", "build"], check=True)
    subprocess.run(["mv", f"dist/{whl}", name], check=True)
    subprocess.run(["uv", "venv"], cwd=f"./{name}", check=True)
    subprocess.run(["uv", "pip", "install", whl], cwd=f"./{name}", check=True)
    subprocess.run(
        ["uv", "run", "kedro", "databricks", "init"], cwd=f"./{name}", check=True
    )
    subprocess.run(
        [
            "uv",
            "run",
            "kedro",
            "databricks",
            "bundle",
            "--overwrite",
        ],
        cwd=f"./{name}",
        check=True,
    )
    subprocess.run(
        [
            "uv",
            "run",
            "kedro",
            "databricks",
            "deploy",
        ],
        cwd=f"./{name}",
        check=True,
    )
    subprocess.run(
        [
            "databricks",
            "bundle",
            "run",
            name.replace("-", "_"),
        ],
        cwd=f"./{name}",
        check=True,
    )
    if destroy:
        subprocess.run(
            ["databricks", "bundle", "destroy", "--auto-approve"],
            cwd=f"./{name}",
            check=True,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Kedro Databricks flow")
    parser.add_argument(
        "--new",
        action="store_true",
        help="Create a new Kedro Databricks project",
    )
    parser.add_argument(
        "--name",
        type=str,
        default="develop-eggs",
        help="Name of the new Kedro Databricks project",
    )
    parser.add_argument(
        "--destroy",
        action="store_true",
        help="Destroy the Databricks bundle after running",
    )
    args = parser.parse_args()
    if args.new:
        new_project(args.name)
    elif Path(f"./{args.name}").exists():
        shutil.rmtree(Path(f"./{args.name}"))
        new_project(args.name)
    run_kedro_databricks_flow(args.name, destroy=args.destroy)
