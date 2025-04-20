import argparse
import logging
from importlib.metadata import version

from kedro_databricks.utils import Command


def new_project(name):
    """Create a new Kedro project using the Databricks starter."""
    Command(["kedro", "new", "--starter", "databricks-iris", "--name", name]).run()


def run_kedro_databricks_flow(name, destroy=False):
    ver = version("kedro_databricks")
    whl = f"kedro_databricks-{ver}-py3-none-any.whl"
    logging.basicConfig(level=logging.INFO)
    Command(["uv", "build"]).run()
    Command(["mv", f"dist/{whl}", name]).run()
    Command(["uv", "venv"]).run(cwd=f"./{name}")
    Command(["uv", "pip", "install", whl]).run(cwd=f"./{name}")
    Command(
        [
            "uv",
            "run",
            "kedro",
            "databricks",
            "init",
            "--provider",
            "azure",
        ]
    ).run(cwd=f"./{name}")
    Command(
        [
            "uv",
            "run",
            "kedro",
            "databricks",
            "bundle",
            "--overwrite",
        ]
    ).run(cwd=f"./{name}")
    Command(
        [
            "uv",
            "run",
            "kedro",
            "databricks",
            "deploy",
        ]
    ).run(cwd=f"./{name}")
    Command(
        [
            "databricks",
            "bundle",
            "run",
            name.replace("-", "_"),
        ]
    ).run(cwd=f"./{name}")
    if destroy:
        Command(
            [
                "databricks",
                "fs",
                "rm",
                "-r",
                f"dbfs:/FileStore/dev/{name.replace('-', '_')}/",
            ]
        ).run(cwd=f"./{name}")
        Command(["databricks", "bundle", "destroy", "--auto-approve"]).run(
            cwd=f"./{name}"
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
    run_kedro_databricks_flow(args.name, destroy=args.destroy)
