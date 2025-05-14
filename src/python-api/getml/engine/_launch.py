# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


import json
import os
import platform
import re
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from inspect import cleandoc
from os import PathLike
from pathlib import Path
from time import sleep
from typing import Dict, List, Optional, Union

import getml.communication as comm
from getml.constants import COMPOSE_FILE_URL, DOCKER_DOCS_URL, INSTALL_DOCS_URL
from getml.version import __version__

OS_NOT_SUPPORTED_NATIVELY_ERROR_MSG_TEMPLATE = cleandoc(
    """
    The operating system '{os}' is not supported natively by getML.

    You can use the dockerized version of getML to run it on your OS.

    Refer to the documentation for more information:
    {docker_docs_url}

    The simplest way to get started is to use the curated compose runtime:
    curl -L {compose_file_url} | docker compose -f - up
    """
)


class Edition(str, Enum):
    ENTERPRISE = "enterprise"
    COMMUNITY = "community"


class Arch(str, Enum):
    AMD64 = "amd64"
    ARM64 = "arm64"


class Os(str, Enum):
    LINUX = "linux"
    MACOS = "darwin"
    WINDOWS = "windows"


NATIVELY_SUPPORTED_OSES = (Os.LINUX,)

EXECUTABLE_NAME = "getML"

SEMVER_REGEX = re.compile(
    r"""
    (?P<version_major>0|[1-9]\d*)
    \.(?P<version_minor>0|[1-9]\d*)
    \.(?P<version_patch>0|[1-9]\d*)
    (?:-(?P<version_prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)
    (?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?
    (?:\+(?P<version_buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?
    """,
    re.VERBOSE,
)
"""
Regex to match semantic version strings. Adapted from:
https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
"""

PACKAGE_NAME_REGEX = re.compile(
    rf"""
    getml-(?P<edition>{'|'.join(Edition)})
    -(?P<version>{SEMVER_REGEX.pattern})
    -(?P<arch>{'|'.join(Arch)})
    -(?P<os>{'|'.join(NATIVELY_SUPPORTED_OSES)})
    """,
    re.VERBOSE,
)
"""
Regex to match getML package names. The package name is expected to have the following format:
getml-{edition}-{version}-{arch}-{os}
"""

INSTALL_LOCATIONS = (
    Path.home() / ".getML",
    Path("/usr/local") / "getML",
    Path(__file__).parent.parent / ".getML",
)
"""
Locations where getML is expected to be installed.
"""

COULD_NOT_FIND_EXECUTABLE_ERROR_MSG_TEMPLATE = cleandoc(
    """
    Could not find getML executable in any of the following locations:
    {install_locations}

    Refer to the installation documentation for more information:
    {install_docs_url}
    """
)

MAX_LAUNCH_WAIT_TIME = 10
"""
Maximum time to wait for the getML subprocess to start (in seconds).
"""

HEALTH_CHECK_INTERVAL = 0.1
"""
Interval between health checks during launch (in seconds).
"""

ENGINE_DID_NOT_RESPOND_IN_TIME_ERROR_MSG_TEMPLATE = cleandoc(
    """
    getML Engine did not respond within {max_launch_wait_time} seconds.

    For furher information, check the log file at {{log_file}}.
    """
).format(max_launch_wait_time=MAX_LAUNCH_WAIT_TIME)


def _handle_option_name(name: str) -> str:
    return name.replace("_", "-")


def _handle_option_value(value: Union[bool, int, str, PathLike, None]) -> str:
    return json.dumps(value, default=str).strip('"')


@dataclass
class _Options:
    allow_push_notifications: bool
    allow_remote_ips: bool
    home_directory: PathLike
    http_port: Optional[int]
    in_memory: bool
    install: bool
    launch_browser: bool
    log: bool
    project_directory: Optional[PathLike]
    proxy_url: Optional[str]
    token: Optional[str]

    def to_cmd(self) -> List[str]:
        """
        Generates the cmd (args) passed to subprocess.Popen.
        """
        options = [
            f"--{_handle_option_name(name)}={_handle_option_value(value)}"
            for (name, value) in asdict(self).items()
            if value is not None
        ]
        return ["./getML", *options]


def create_log_dir(getml_home_directory: Path) -> Path:
    log_path = getml_home_directory / "logs"
    log_path.mkdir(parents=True, exist_ok=True)
    return log_path


def locate_installed_packages_by_edition() -> Dict[Edition, List[Path]]:
    """
    Locate all installed getML packages that match the python api's version
    grouped by edition.
    """
    installed_packages = {edition: [] for edition in Edition}
    for location in INSTALL_LOCATIONS:
        for package in location.glob("getml-*"):
            if package.is_dir():
                match = PACKAGE_NAME_REGEX.match(package.name)
                if match and match.group("version") == __version__:
                    installed_packages[Edition(match.group("edition"))].append(package)
    return installed_packages


def locate_executable() -> Optional[Path]:
    """
    Locates the getML executable respecting the prioitized order of:
    1. editions (enterprise > community)
    2. instalation locations (home > /usr/local > getml package directory)

    Returns the first found executable or None if none is found.
    """
    installed_packages = locate_installed_packages_by_edition()
    for edition in Edition:
        packages = installed_packages[edition]
        for package in packages:
            for executable in package.glob(EXECUTABLE_NAME):
                if (
                    executable.name == EXECUTABLE_NAME
                    and executable.is_file()
                    and os.access(executable, os.X_OK)
                ):
                    return executable


def launch(
    allow_push_notifications: bool = True,
    allow_remote_ips: bool = False,
    home_directory: Optional[str] = None,
    http_port: Optional[int] = None,
    in_memory: bool = True,
    install: bool = False,
    launch_browser: bool = True,
    log: bool = False,
    project_directory: Optional[Union[PathLike, str]] = None,
    proxy_url: Optional[str] = None,
    token: Optional[str] = None,
    quiet: bool = False,
):
    """
    Launches the getML Engine.

    Args:
      allow_push_notifications:
        Whether you want to allow the getML Monitor to send push notifications to your desktop.

      allow_remote_ips:
        Whether you want to allow remote IPs to access the http-port.

      home_directory:
        The directory which should be treated as the home directory by getML.
        getML will create a hidden folder named '.getML' in said directory.
        All binaries will be installed there.

      http_port:
        The local port of the getML Monitor.
        This port can only be accessed from your local computer,
        unless you set `allow_remote_ips=True`.

      in_memory:
        Whether you want the Engine to process everything in memory.

      install:
        Reinstalls getML, even if it is already installed.

      launch_browser:
        Whether you want to automatically launch your browser.

      log:
        Whether you want the Engine log to appear in the logfile (more detailed logging).
        The Engine log also appears in the 'Log' page of the Monitor.

      project_directory:
        The directory in which to store all of your projects.

      proxy_url:
        The URL of any proxy server that that redirects to the getML Monitor.

      token:
        The token used for authentication.
        Authentication is required when remote IPs are allowed to access the Monitor.
        If authentication is required and no token is passed,
        a random hexcode will be generated as the token.

      quiet:
        Whether to suppress output.
    """

    if comm.is_monitor_alive():
        print("getML Engine is already running.")
        return
    if (os_ := platform.system().lower()) not in NATIVELY_SUPPORTED_OSES:
        raise OSError(
            OS_NOT_SUPPORTED_NATIVELY_ERROR_MSG_TEMPLATE.format(
                os=os_,
                docker_docs_url=DOCKER_DOCS_URL,
                compose_file_url=COMPOSE_FILE_URL,
            )
        )
    executable_path = locate_executable()
    if not executable_path:
        raise OSError(
            COULD_NOT_FIND_EXECUTABLE_ERROR_MSG_TEMPLATE.format(
                install_locations=[str(p) for p in INSTALL_LOCATIONS],
                install_docs_url=INSTALL_DOCS_URL,
            )
        )
    getml_dir = (
        Path(home_directory) if home_directory is not None else Path.home() / ".getML"
    )
    project_dir = (
        Path(project_directory)
        if project_directory is not None
        else getml_dir / "projects"
    )
    log_dir = create_log_dir(getml_dir)
    log_file = log_dir / f"getml_{datetime.now():%Y%m%d%H%M%S}.log"
    cmd = _Options(
        allow_push_notifications=allow_push_notifications,
        allow_remote_ips=allow_remote_ips,
        home_directory=getml_dir,
        http_port=http_port,
        in_memory=in_memory,
        install=install,
        launch_browser=launch_browser,
        log=log,
        project_directory=project_dir,
        proxy_url=proxy_url,
        token=token,
    ).to_cmd()
    if not quiet:
        print(f"Launching {' '.join(cmd)} in {executable_path.parent}...")
    subprocess.Popen(
        cmd,
        cwd=executable_path.parent,
        shell=False,
        stdout=log_file.open("w"),
        stderr=subprocess.STDOUT,
    )
    for _ in range(int(MAX_LAUNCH_WAIT_TIME / HEALTH_CHECK_INTERVAL)):
        if comm.is_monitor_alive():
            if not quiet:
                print(
                    f"Launched the getML Engine. The log output will be stored in {log_file}"
                )
            return
        sleep(HEALTH_CHECK_INTERVAL)
    raise TimeoutError(
        ENGINE_DID_NOT_RESPOND_IN_TIME_ERROR_MSG_TEMPLATE.format(log_file=log_file)
    )
