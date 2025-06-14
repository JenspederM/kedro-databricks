from pathlib import Path

import mkdocs_gen_files

mkdocs_gen_files.log.info("Generating changelog...")
root = Path(__file__).parent.parent

with mkdocs_gen_files.open("changelog.md", "w") as f:
    changelog_file = root / "CHANGELOG.md"
    f.write(changelog_file.read_text())
