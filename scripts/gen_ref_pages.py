from pathlib import Path

import mkdocs_gen_files

root = Path(__file__).parent.parent
src = root / "src"

for path in sorted([f for f in src.rglob("*.py") if f.name != "databricks_run.py"]):
    module_path = path.relative_to(src).with_suffix("")
    module_name = module_path.name
    identifier = ".".join([part for part in module_path.parts if part != "__init__"])
    docname = module_name if module_name != "__init__" else "index"
    mkdocs_gen_files.log.info(
        f"{module_path}: Generating reference page for {identifier} - {docname}"
    )

    doc_path = (module_path.parent / docname).with_suffix(".md")
    full_doc_path = Path("reference", doc_path)

    if module_path.name == "__main__":
        continue

    if identifier in ["kedro_databricks", "kedro_databricks.cli"]:
        continue

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        print("::: " + identifier, file=fd)

    mkdocs_gen_files.set_edit_path(full_doc_path, path.relative_to(root))
