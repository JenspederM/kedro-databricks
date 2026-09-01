from pathlib import Path

root = Path(__file__).parent.parent
examples_dir = root / "examples"
examples_doc = root / "docs" / "examples.md"


def main():
    """Generate the code reference pages."""
    examples = sorted(examples_dir.iterdir())
    current_example_page = examples_doc.read_text() if examples_doc.exists() else ""
    new_example_content = ["# Examples\n\n"]

    for p in examples:
        if not p.is_dir() or p.name.startswith("."):
            continue

        example = _load_example(p)

        new_example_content.append(f"## {p.name}\n\n" + "\n".join(example))

        if p != examples[-1]:
            new_example_content.append("\n\n")
        else:
            new_example_content.append("\n")

    new_example_page = "".join(new_example_content)
    if new_example_page != current_example_page:
        print(f"Updating {examples_doc}")  # noqa: T201
        with open(examples_doc, "w") as f:
            f.write(new_example_page)
    else:
        print(f"No changes detected in {examples_doc}")  # noqa: T201


def _load_data(p: Path):
    description = ""
    if (p / "README.md").exists():
        description = (p / "README.md").read_text().strip()
    databricks_config = _parse_file(p / "databricks.yml")
    resources_config = _parse_file(p / "resources.yml")
    result_config = _parse_file(p / "result.yml")
    return databricks_config, resources_config, result_config, description


def _load_example(p: Path) -> list[str]:
    """Load an example from the given path."""
    databricks_config, resources_config, result_config, description = _load_data(p)
    result_highlight = _get_hl_lines(resources_config, result_config)
    parts = []
    parts.append(description)
    parts.append("")
    parts.append('=== "conf/`[env]`/databricks.yml"')
    parts.append("    ```yaml")
    parts.extend(_add_padding(databricks_config, leftpad=4))
    parts.append("    ```")
    parts.append("")
    parts.append('=== "Before: resources/`[pipeline]`.yml"')
    parts.append("    ```yaml")
    parts.extend(_add_padding(resources_config, leftpad=4))
    parts.append("    ```")
    parts.append("")
    parts.append('=== "After: resources/`[pipeline]`.yml"')
    parts.append(f'    ```yaml hl_lines="{result_highlight}"')
    parts.extend(_add_padding(result_config, leftpad=4))
    parts.append("    ```")
    return parts


def _get_hl_lines(old, new):
    old = old.copy()
    new = new.copy()
    i, j = 0, 0
    hl_lines = []
    while j < len(new):
        i = min(i, len(old) - 1)
        if old[i] != new[j]:
            # +1 for 1-based indexing in markdown hl_lines
            hl_lines.append(j + 1)
            j += 1
        elif old[i] == new[j]:
            i += 1
            j += 1
        else:
            raise ValueError("Unexpected case in diffing")
    return " ".join(str(x) for x in hl_lines)


def _parse_file(file_path: Path) -> list[str]:
    """Parse a file and return its content."""
    return [line for line in file_path.read_text().strip().splitlines()]


def _add_padding(lines: list[str], leftpad: int = 4) -> list[str]:
    """Add padding to each line in the list."""
    padding = " " * leftpad
    return [padding + line.rstrip() for line in lines]


if __name__ == "__main__":
    main()
