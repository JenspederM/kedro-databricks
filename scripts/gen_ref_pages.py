from pathlib import Path

import mkdocs_gen_files

root = Path(__file__).parent.parent
src = root / "src"

for path in sorted([f for f in src.rglob("*.py") if f.name != "databricks_run.py"]):
    module_path = path.relative_to(src).with_suffix("")
    doc_path = path.relative_to(src).with_suffix(".md")
    full_doc_path = Path("reference", doc_path)

    parts = tuple(module_path.parts)

    if module_path.name in ["utils"]:
        continue

    if module_path.name == "__init__":
        try:
            size = path.read_bytes()
            if not size:
                continue
        except Exception:
            pass

        parts = parts[:-1]
    elif parts[-1] == "__main__":
        continue

    identifier = ".".join(parts)

    if identifier in ["kedro_databricks", "kedro_databricks.cli"]:
        continue

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        identifier = ".".join(parts)
        print("::: " + identifier, file=fd)

    mkdocs_gen_files.set_edit_path(full_doc_path, path.relative_to(root))
