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

        description = ""
        if (p / "README.md").exists():
            description = (p / "README.md").read_text().strip()

        parts = [
            f"# {example_name.title().replace('_', ' ')}",
            "",
            description,
            "",
            '=== "conf/[env]/databricks.yml"',
            "    ```yaml",
            *[
                "    " + line
                for line in (p / "databricks.yml").read_text().strip().splitlines()
            ],
            "    ```",
            "",
            '=== "Before: resources/<pipeline>.yml"',
            "    ```yaml",
            *[
                "    " + line
                for line in (p / "resources.yml").read_text().strip().splitlines()
            ],
            "    ```",
            "",
            '=== "After: resources/<pipeline>.yml"',
            "    ```yaml",
            *[
                "    " + line
                for line in (p / "result.yml").read_text().strip().splitlines()
            ],
            "    ```",
        ]
        with mkdocs_gen_files.open(f"examples/{example_name}.md", "w") as f:
            f.write("\n".join(parts) + "\n")
    pass


gen()
