from pathlib import Path

import mkdocs_gen_files

mkdocs_gen_files.log.info("Generating example pages...")
root = Path(__file__).parent.parent
examples_dir = root / "examples"


def gen():
    """Generate the code reference pages."""
    for p in sorted(examples_dir.iterdir()):
        example_name = p.name
        if not p.is_dir() or example_name.startswith("."):
            continue

        example = _load_example(p)

        with mkdocs_gen_files.open(f"examples/{example_name}.md", "w") as f:
            f.write("\n".join(example) + "\n")
    pass


def _load_example(p: Path) -> list[str]:
    example_name = p.name
    description = ""
    if (p / "README.md").exists():
        description = (p / "README.md").read_text().strip()

    databricks_config = _parse_file(p / "databricks.yml")
    resources_config = _parse_file(p / "resources.yml")
    result_config = _parse_file(p / "result.yml")
    result_diff = _add_diff(resources_config, result_config)

    parts = [
        f"# {example_name.title().replace('_', ' ')}",
        "",
        description,
        "",
        '=== "conf/[env]/databricks.yml"',
        "    ```yaml",
        *_add_padding(databricks_config, leftpad=4),
        "    ```",
        "",
        '=== "Before: resources/<pipeline>.yml"',
        "    ```yaml",
        *_add_padding(resources_config, leftpad=4),
        "    ```",
        "",
        '=== "After: resources/<pipeline>.yml"',
        "    ```diff",
        *_add_padding(result_diff, leftpad=4),
        "    ```",
    ]
    return parts


def _add_diff(old, new):
    i, j = 0, 0
    diffed = []
    while j < len(new):
        i = min(i, len(old) - 1)
        if old[i] != new[j]:
            diffed.append("+" + new[j][1:])
            j += 1
        elif old[i] == new[j]:
            diffed.append(new[j])
            i += 1
            j += 1
        else:
            raise ValueError("Unexpected case in diffing")
    return diffed


def _parse_file(file_path: Path, leftpad: int = 4) -> list[str]:
    """Parse a file and return its content."""
    return [line for line in file_path.read_text().strip().splitlines()]


def _add_padding(lines: list[str], leftpad: int = 4) -> list[str]:
    """Add padding to each line in the list."""
    padding = " " * leftpad
    return [padding + line for line in lines]


gen()
