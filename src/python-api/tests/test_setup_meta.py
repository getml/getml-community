# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

import os
import re
from typing import List

import pytest


@pytest.fixture(name="toml_content", scope="session")
def fixture_toml_content() -> List[str]:
    path = os.path.dirname(__file__)
    with open(
        os.path.join(path, "..", "pyproject.toml"), "r", encoding="utf-8"
    ) as file:
        return file.readlines()


@pytest.fixture(name="package_version", scope="session")
def fixture_package_version(toml_content) -> str:
    for line in toml_content:
        match = re.match(r"version\s*=\s*\"([\d.]+)\"", line)
        if match:
            return match.group(1)
    raise RuntimeError("Couldn't find version string in toml.")


def test_documentation_url_in_toml(toml_content, package_version):
    merged_content = "".join(toml_content)
    pattern = (
        r"\"Documentation\" = \"https:\/\/docs.getml.com\/" + package_version + r"\""
    )
    search = re.search(pattern, merged_content)
    assert search, "Documentation url in pyproject.toml doesn't match version."


def test_documentation_url_in_readme(package_version):
    path = os.path.dirname(__file__)
    with open(os.path.join(path, "..", "README.md"), "r", encoding="utf-8") as file:
        lines = file.readlines()
    url_lines = [_ for _ in lines if r"https://docs.getml.com/" in _]
    pattern = r"https:\/\/docs.getml.com\/" + package_version
    result = all(re.search(pattern, _) for _ in url_lines)
    assert result, "Versionated url in README.md doesn't match version."
