import argparse
import logging
import shutil
import subprocess
import sys
from pathlib import Path

import tomlkit

root = Path(__file__).parent.parent
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


def main():
    args = parse_args(sys.argv[1:])
    match args.command:
        case "sync":
            sync(Path(args.name))
        case "new":
            new(args.name, root / args.name, args.overwrite)
        case _:
            log.error(f"Unknown command: {args.command}")
            sys.exit(1)


def current_version() -> str:
    """Get the current version of the project."""
    pyproject = root / "pyproject.toml"

    if not pyproject.exists():
        raise FileNotFoundError(f"pyproject.toml not found at {pyproject}.")

    with open(pyproject, "rb") as f:
        project = tomlkit.load(f)

    return project.get("project", {}).get("version", "0.0.0")


def parse_args(args: list[str]):
    parser = argparse.ArgumentParser(description="Create a new development project")
    subparsers = parser.add_subparsers(dest="command")

    sync_parser = subparsers.add_parser("sync")

    for obj in [sync_parser]:
        obj.add_argument(
            "name", type=str, default="develop-eggs", help="Name of the project"
        )

    new_parser = subparsers.add_parser("new")
    for obj in [new_parser]:
        obj.add_argument(
            "name", type=str, default="develop-eggs", help="Name of the project"
        )
        obj.add_argument(
            "-o",
            "--overwrite",
            action="store_true",
            help="Overwrite the project if it already exists",
            default=False,
        )

    return parser.parse_args()


def new(project_name: str, project_path: Path, overwrite: bool = False):
    if project_path.exists() and not overwrite:
        log.error(f"Project '{project_name}' already exists at {project_path}.")
    elif project_path.exists() and overwrite:
        log.warning(
            f"Project '{project_name}' already exists at {project_path}. Overwriting."
        )
        shutil.rmtree(project_path)
    log.info(f"Created project '{project_name}' at {project_path}.")
    subprocess.run(
        [
            "uv",
            "run",
            "kedro",
            "new",
            "--starter",
            "databricks-iris",
            "--name",
            project_name,
        ],
        check=False,
    )

    with open(project_path / "pyproject.toml", "rb") as f:
        project = tomlkit.load(f)

    project["project"]["requires-python"] = ">=3.10"  # type: ignore

    with open(project_path / "pyproject.toml", "w") as f:
        tomlkit.dump(project, f)

    Path(project_path / ".tool-versions").write_text("uv 0.7.8\n")
    Path(project_path / ".python-version").write_text("3.11\n")
    sync(project_path)


def sync(project_path: Path):
    """Synchronize the development environment."""
    whl_name = f"kedro_databricks-{current_version()}-py3-none-any.whl"
    whl_src = root / "dist" / whl_name
    whl_dst = project_path / whl_name
    log.info(f"Synchronizing development environment for project at {project_path}.")
    subprocess.run(["uv", "build"], check=False, cwd=root)
    shutil.copy(whl_src, whl_dst)
    subprocess.run(["uv", "add", whl_name, "--upgrade"], check=False, cwd=project_path)
    log.info("Development environment synchronized.")


if __name__ == "__main__":
    main()
