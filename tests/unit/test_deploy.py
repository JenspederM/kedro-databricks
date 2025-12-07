from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class MockResult:
    stdout: list[str]
    stderr: list[str]


class MetadataMock:
    def __init__(self, path: str, name: str):
        self.project_path = Path(path)
        self.project_name = name
        self.package_name = name
        self.source_dir = "src"
        self.env = "local"
        self.config_file = "conf/base"
        self.project_version = "0.16.0"
        self.project_description = "Test Project Description"
        self.project_author = "Test Author"
        self.project_author_email = "author@email.com"
