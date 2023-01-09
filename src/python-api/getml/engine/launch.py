# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


from datetime import datetime
from os import makedirs, listdir
from os.path import isdir
from functools import reduce, partial
from pathlib import Path
from platform import system
import socket
from subprocess import Popen
from time import sleep
from typing import List, NamedTuple, Optional, Union, Dict

from getml.communication import tcp_port
from getml.version import __version__


class _Options(NamedTuple):
    allow_push_notifications: bool
    allow_remote_ips: bool
    home_directory: Optional[str]
    http_port: Optional[int]
    in_memory: bool
    install: bool
    launch_browser: bool
    log: bool
    project_directory: Optional[str]
    proxy_url: Optional[str]
    token: Optional[str]

    def to_cmd(self, binary_path: Path) -> List[str]:
        """
        Generates the command to be passed to Popen.
        """
        return ["./getML"] + [
            "--" + key.replace("_", "-") + "=" + _handle_value(value)
            for (key, value) in self._asdict().items()  # pylint: disable=E1101
            if value is not None
        ]


def _make_home_path(home_directory: Optional[str]) -> Path:
    if home_directory is not None:
        return Path(home_directory)
    paths = [Path.home(), Path("/usr/local"), Path(__file__).parent.parent]
    home_paths = [p for p in paths if _try_find_binary(p) is not None]
    if home_paths:
        return home_paths[0]
    raise OSError(
        "Could not find an installation of the getML binary in "
        + f"any of the following locations: {[str(p) for p in paths]}"
    )


def _map_version_directories(
    acc: Dict[str, List[Path]],
    directory: Path,
    getml_directory: Path,
) -> Dict[str, List[Path]]:
    if not isdir(getml_directory / directory) or f"getml-{__version__}" not in str(
        directory
    ):
        return acc

    key = "community-edition" in str(directory) and "community" or "enterprise"
    acc.setdefault(key, []).append(directory)
    return acc


def _extract_getml_exec_path(getml_directory: Path, directories: List[Path]) -> Path:
    if len(directories) > 1:
        raise OSError(
            "More than one installation found for getml-"
            + __version__
            + " in "
            + str(getml_directory)
            + "."
        )

    if system() == "Linux":
        return getml_directory / directories[0] / "getML"
    return getml_directory / directories[0] / "getml-cli"


def _try_find_binary(home_directory: Path) -> Optional[Path]:
    try:
        _find_binary(home_directory)
        return home_directory
    except OSError:
        return None


def _find_binary(home_directory: Path) -> Path:
    getml_directory = (
        home_directory / ".getML"
        if home_directory != Path("/usr/local")
        else home_directory / "getML"
    )

    if not getml_directory.exists():
        raise OSError(f"{getml_directory} does not exist.")

    _reduce = partial(
        _map_version_directories,
        getml_directory=getml_directory,
    )

    version_directories: Dict[str, List[Path]] = reduce(
        _reduce, listdir(getml_directory), {}
    )

    if not version_directories:
        raise OSError(
            "getml-"
            + __version__
            + " is not installed in "
            + str(getml_directory)
            + "."
        )

    enterprise_directories = version_directories.get("enterprise", [])
    community_directories = version_directories.get("community", [])

    if enterprise_directories:
        return _extract_getml_exec_path(
            getml_directory,
            enterprise_directories,
        )

    return _extract_getml_exec_path(getml_directory, community_directories)


def _handle_bool(value: bool) -> str:
    if value:
        return "true"
    return "false"


def _handle_value(value: Union[str, int, bool]) -> str:
    if isinstance(value, bool):
        return _handle_bool(value)
    return str(value)


def _make_log_path(home_directory: Path) -> Path:
    getml_directory = (
        home_directory / ".getML"
        if home_directory != Path("/usr/local")
        and home_directory != Path(__file__).parent.parent
        else Path.home() / ".getML"
    )
    makedirs(getml_directory / "logs", exist_ok=True)
    return getml_directory / "logs" / datetime.now().strftime("%Y%m%d%H%M%S.log")


def _is_monitor_alive() -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(("localhost", tcp_port))
    except ConnectionRefusedError:
        return False
    return True


def launch(
    allow_push_notifications=True,
    allow_remote_ips=False,
    home_directory: Optional[str] = None,
    http_port: Optional[int] = None,
    in_memory=True,
    install=False,
    launch_browser=True,
    log=False,
    project_directory: Optional[str] = None,
    proxy_url: Optional[str] = None,
    token: Optional[str] = None,
):
    """
    Launches the getML engine.

    Args:
      allow_push_notifications (bool):
            Whether you want to allow the getML monitor to send push notifications to your desktop.

      allow_remote_ips (bool):
            Whether you want to allow remote IPs to access the http-port.

      home_directory (str, optional):
            The directory which should be treated as the home directory by getML.
            getML will create a hidden folder named '.getML' in said directory.
            All binaries will be installed there.

      http_port (int, optional):
            The local port of the getML monitor.
            This port can only be accessed from your local computer,
            unless you set allow_remote_ips=True.

      in_memory (bool):
            Whether you want the engine to process everything in memory.

      install (bool)
            Reinstalls getML, even if it is already installed.

      launch_browser (bool):
            Whether you want to automatically launch your browser.

      log (bool):
            Whether you want the engine log to appear in the logfile (more detailed logging).
            The engine log also appears in the 'Log' page of the monitor.

      project_directory (str, optional):
            The directory in which to store all of your projects.

      proxy_url (str, optional):
            The URL of any proxy server that that redirects to the getML monitor.

      tcp_port (int, optional):
            Local TCP port which serves as the communication point for the engine.
            This port can only be accessed from your local computer.

      token (str, optional):
            The token used for authentication.
            Authentication is required when remote IPs are allowed to access the monitor.
            If authentication is required and no token is passed,
            a random hexcode will be generated as the token."""
    if _is_monitor_alive():
        print("getML engine is already running.")
        return
    home_path = _make_home_path(home_directory)
    binary_path = _find_binary(home_path)
    log_path = _make_log_path(home_path)
    logfile = open(str(log_path), "w", encoding="utf-8")
    cmd = _Options(
        allow_push_notifications=allow_push_notifications,
        allow_remote_ips=allow_remote_ips,
        home_directory=str(home_path),
        http_port=http_port,
        in_memory=in_memory,
        install=install,
        launch_browser=launch_browser,
        log=log,
        project_directory=project_directory,
        proxy_url=proxy_url,
        token=token,
    ).to_cmd(binary_path)
    cwd = str(binary_path.parent)
    print(f"Launching {' '.join(cmd)} in {cwd}...")
    Popen(cmd, cwd=cwd, shell=False, stdout=logfile, stdin=logfile, stderr=logfile)
    while not _is_monitor_alive():
        sleep(0.1)
    print(f"Launched the getML engine. The log output will be stored in {log_path}.")
