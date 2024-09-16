from __future__ import annotations

import fnmatch
import itertools as it
from typing import List, Type

import pytest


class FakePath:
    valid_paths: List[str] = []
    valid_files: List[str] = []

    def __init__(self, path: str):
        self._path = path

    def __eq__(self, other: FakePath) -> bool:
        return self._path == other._path

    def __str__(self) -> str:
        return self._path

    def __repr__(self) -> str:
        return f"FakePath({self._path})"

    def __fspath__(self) -> str:
        return self._path

    def exists(self) -> bool:
        return self._path in FakePath.valid_paths or self._path in FakePath.valid_files

    def glob(self, pattern: str) -> List[FakePath]:
        pattern_abs = f"{self._path}/{pattern}"
        return [
            FakePath(path)
            for path in it.chain(self.valid_paths, self.valid_files)
            if fnmatch.fnmatch(path, pattern_abs)
        ]

    def is_file(self) -> bool:
        return self._path in FakePath.valid_files

    def is_dir(self) -> bool:
        return self._path in FakePath.valid_paths

    @property
    def name(self) -> str:
        return next(reversed(self._path.split("/")), self._path)

    def read_text(self) -> str:
        if self._path in FakePath.valid_files:
            return "fake file content for " + self._path
        else:
            raise FileNotFoundError(f"No such file: '{self._path}'")

    def write_text(self, content) -> None:
        if self._path in FakePath.valid_files:
            print(f"Writing '{content}' to fake path {self._path}")
        else:
            FakePath.valid_files.append(self._path)
            print(f"Creating and writing '{content}' to fake path {self._path}")


@pytest.fixture
def fake_path(request) -> Type[FakePath]:
    FakePath.valid_paths = request.param.get("valid_paths", [])
    FakePath.valid_files = request.param.get("valid_files", [])
    return FakePath
