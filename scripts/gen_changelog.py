import subprocess
import tempfile
from pathlib import Path

import mkdocs_gen_files

mkdocs_gen_files.log.info("Generating changelog...")


tmp = Path(tempfile.mktemp()).with_suffix(".md")
subprocess.run(["uv", "run", "cz", "ch", "--file-name", tmp.as_posix()], check=False)
with mkdocs_gen_files.open("changelog.md", "w") as f:
    f.write(tmp.read_text())
mkdocs_gen_files.log.info("Changelog generated successfully.")

tmp.unlink(missing_ok=True)  # Clean up the temporary file
mkdocs_gen_files.log.info("Temporary file cleaned up.")
