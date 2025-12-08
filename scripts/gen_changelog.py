import subprocess
import tempfile
from pathlib import Path

import mkdocs_gen_files

mkdocs_gen_files.log.info("Generating changelog...")

with tempfile.NamedTemporaryFile(suffix=".md", delete=False) as tmp:
    tmp_path = Path(tmp.name)
    subprocess.run(
        [
            "uv",
            "run",
            "cz",
            "ch",
            "--file-name",
            tmp_path.as_posix(),
        ],
        check=False,
    )
    with mkdocs_gen_files.open("changelog.md", "w") as f:
        f.write(tmp_path.read_text())

mkdocs_gen_files.log.info("Changelog generated successfully.")
tmp_path.unlink(missing_ok=True)  # Clean up the temporary file
mkdocs_gen_files.log.info("Temporary file cleaned up.")
