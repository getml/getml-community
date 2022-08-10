"""
This takes care of the migration
from the monorepo to the getml-community
repo.
"""

import os
from pathlib import Path
import shutil


BLACKLIST = [
    ".codelite",
    ".git",
    ".gitignore",
    ".mypy_cache",
    ".ipynb_checkpoints",
    ".vscode",
    "bin",
    "pkg",
    "build",
    "monitor",
    "dependencies",
    "documentation",
    "frontend",
    "goutils",
    "linux-x64",
    "license_bot",
    "migration.py",
    "compile_commands.json",
    "get_compile_commands.sh",
]


def _copy_files(current_path: Path, target_path: Path):
    fnames = os.listdir(str(current_path))
    for fname in fnames:
        if fname in BLACKLIST:
            continue
        if os.path.isdir(str(current_path / fname)):
            _copy_directory(current_path / fname, target_path / fname)
        else:
            shutil.copy2(str(current_path / fname), str(target_path / fname))


def _copy_directory(current_path: Path, target_path: Path):
    if os.path.isdir(str(target_path)):
        shutil.rmtree(str(target_path))
    os.makedirs(str(target_path))
    _copy_files(current_path, target_path)


if __name__ == "__main__":
    _copy_files(Path("."), Path("../getml-community"))
