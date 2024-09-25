from unittest.mock import patch

import pytest

from getml.engine import Edition
from getml.engine._launch import locate_executable, locate_installed_packages_by_edition

from .conftest import FakePath


def access(path: str, mode: str):
    return True


@pytest.mark.parametrize(
    "fake_path, install_locations, expected",
    [
        (
            {
                "valid_paths": [
                    "/fake/home/.getML/getml-community-1.2.3-amd64-linux",
                    "/fake/usr/local/getML/getml-enterprise-1.2.3-amd64-linux",
                ],
                "valid_files": [],
            },
            [FakePath("/fake/home/.getML"), FakePath("/fake/usr/local/getML")],
            {
                Edition.COMMUNITY: [
                    FakePath("/fake/home/.getML/getml-community-1.2.3-amd64-linux")
                ],
                Edition.ENTERPRISE: [
                    FakePath("/fake/usr/local/getML/getml-enterprise-1.2.3-amd64-linux")
                ],
            },
        ),
        (
            {
                "valid_paths": [
                    "/fake/home/.getML/getml-community-1.2.2-amd64-linux",
                    "/fake/home/.getML/getml-community-1.2.3-amd64-linux",
                ],
                "valid_files": [],
            },
            [FakePath("/fake/home/.getML")],
            {
                Edition.COMMUNITY: [
                    FakePath("/fake/home/.getML/getml-community-1.2.3-amd64-linux")
                ],
                Edition.ENTERPRISE: [],
            },
        ),
    ],
    indirect=["fake_path"],
)
def test_locate_installed_packages_by_edition(fake_path, install_locations, expected):
    # fmt: off
    with patch("getml.engine._launch.INSTALL_LOCATIONS", install_locations),\
         patch("getml.engine._launch.__version__", "1.2.3"),\
         patch("os.access", access):
    # fmt: on
            assert locate_installed_packages_by_edition() == expected


@pytest.mark.parametrize(
    "fake_path, install_locations, expected",
    [
        (
            {
                "valid_paths": [
                    "/fake/home/.getML/getml-community-1.2.3-amd64-linux",
                    "/fake/usr/local/getML/getml-community-1.2.3-amd64-linux",
                ],
                "valid_files": [
                    "/fake/home/.getML/getml-community-1.2.3-amd64-linux/getML",
                    "/fake/usr/local/getML/getml-community-1.2.3-amd64-linux/getML",
                ],
            },
            [FakePath("/fake/home/.getML"), FakePath("/fake/usr/local/getML")],
            FakePath("/fake/home/.getML/getml-community-1.2.3-amd64-linux/getML"),
        ),
        (
            {
                "valid_paths": [
                    "/fake/home/.getML/getml-enterprise-1.2.3-amd64-linux",
                    "/fake/usr/local/getML/getml-enterprise-1.2.3-amd64-linux",
                ],
                "valid_files": [
                    "/fake/home/.getML/getml-enterprise-1.2.3-amd64-linux/getML",
                    "/fake/usr/local/getML/getml-enterprise-1.2.3-amd64-linux/getML",
                ],
            },
            [FakePath("/fake/home/.getML"), FakePath("/fake/usr/local/getML")],
            FakePath("/fake/home/.getML/getml-enterprise-1.2.3-amd64-linux/getML"),
        ),
        (
            {
                "valid_paths": [
                    "/fake/usr/local/getML/getml-enterprise-1.2.3-amd64-linux",
                    "/fake/home/.getML/getml-community-1.2.3-amd64-linux",
                ],
                "valid_files": [
                    "/fake/usr/local/getML/getml-enterprise-1.2.3-amd64-linux/getML",
                    "/fake/home/.getML/getml-community-1.2.3-amd64-linux/getML",
                ],
            },
            [FakePath("/fake/home/.getML"), FakePath("/fake/usr/local/getML")],
            FakePath("/fake/usr/local/getML/getml-enterprise-1.2.3-amd64-linux/getML"),
        ),
        (
            {
                "valid_paths": [
                    "/fake/home/.getML/getml-community-1.2.3-amd64-linux",
                ],
                "valid_files": [
                    "/fake/home/.getML/getml-community-1.2.3-amd64-linux/getML",
                ],
            },
            [FakePath("/fake/home/.getML")],
            FakePath("/fake/home/.getML/getml-community-1.2.3-amd64-linux/getML"),
        ),
        (
            {
                "valid_paths": [],
                "valid_files": [],
            },
            [],
            None,
        ),
    ],
    indirect=["fake_path"],
)
def test_locate_executable(fake_path, install_locations, expected):
    # fmt: off
    with patch("getml.engine._launch.INSTALL_LOCATIONS", install_locations),\
         patch("getml.engine._launch.__version__", "1.2.3"),\
         patch("os.access", access):
    # fmt: on
        assert locate_executable() == expected
