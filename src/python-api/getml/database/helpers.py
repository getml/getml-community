# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Collection of helper functions not meant to be used by the enduser.
"""

from enum import Enum
from inspect import cleandoc
from pathlib import Path
from tempfile import gettempdir
from typing import Any, Dict, Iterable, List, Literal, Optional, Union, overload
from urllib import request
from urllib.parse import urlparse

from rich import print

import getml.communication as comm
from getml.database.connection import Connection
from getml.utilities.progress import Progress, ProgressType

FILE_SYSTEM_ERROR_MSG_TEMPLATE = cleandoc(
    """
    Engine could not retrieve files:
    {fnames}

    Is the Engine running inside docker? When ingesting csvs to databases all
    paths in `fnames` are interpreted as paths w.r.t. the container's filesystem.

    Hint: Use `docker cp <container>:/home/getml/assets` to copy the files into
    the docker container and read the data from the container's filesystem
    directly:

    `getml.database.read_csv('table_name', '/home/getml/assets/file.csv')`
    """
)

# -----------------------------------------------------------------------------


class CSVCmdType(Enum):
    SNIFF = "sniff"
    READ = "read"


# -----------------------------------------------------------------------------


def _load_to_file(url: str, file_path: Path, description: Optional[str] = None):
    with request.urlopen(url) as response:
        content_length = int(response.getheader("content-length", 0))
        block_size = max(4096, content_length // 100)
        with Progress(
            progress_type=ProgressType.DOWNLOAD,
        ) as progress:
            task_id = progress.new_task(
                f"Downloading {description or file_path.name}...", total=content_length
            )
            with file_path.open("wb") as file:
                while True:
                    block = response.read(block_size)
                    if not block:
                        break
                    file.write(block)
                    progress.advance(task_id, steps=len(block))


# --------------------------------------------------------------------


def _retrieve_temp_dir() -> Path:
    temp_dir = Path(gettempdir()) / "getml"
    temp_dir.mkdir(exist_ok=True)
    return temp_dir


# --------------------------------------------------------------------


def _retrieve_url(
    url: str,
    verbose: bool = False,
    target_path: Optional[Path] = None,
    description: Optional[str] = None,
) -> str:
    parse_result = urlparse(url)

    if target_path is None:
        target_path = _retrieve_temp_dir()

    target_path = target_path / parse_result.netloc / parse_result.path[1:]

    if target_path.exists():
        return target_path.as_posix()

    target_path.parent.mkdir(parents=True, exist_ok=True)

    if verbose:
        print(f"Downloading {url} to {target_path.as_posix()}...")

    _load_to_file(url, target_path, description)

    return target_path.as_posix()


# --------------------------------------------------------------------


def _retrieve_urls(
    fnames: Iterable[str],
    verbose: bool = False,
    target_path: Optional[Path] = None,
    description: Optional[str] = None,
) -> List[str]:
    def is_url(fname):
        return urlparse(fname).scheme != ""

    return [
        _retrieve_url(
            url=fname, verbose=verbose, target_path=target_path, description=description
        )
        if is_url(fname)
        else Path(fname).expanduser().absolute().as_posix()
        for fname in fnames
    ]


# --------------------------------------------------------------------


def _retrieve_urls_for_engine(fnames: Iterable[str], verbose: bool = True) -> List[str]:
    try:
        fnames = _retrieve_urls(fnames, verbose)
    except FileNotFoundError as e:
        raise OSError(FILE_SYSTEM_ERROR_MSG_TEMPLATE.format(fnames=fnames)) from e

    return fnames


# --------------------------------------------------------------------


@overload
def _read_csv(
    type: Literal[CSVCmdType.SNIFF],
    name: str,
    fnames: Union[str, List[str]],
    num_lines_sniffed: int,
    quotechar: str,
    sep: str,
    skip: int,
    colnames: Optional[List[str]],
    conn: Optional[Connection],
) -> str: ...


@overload
def _read_csv(
    type: Literal[CSVCmdType.READ],
    name: str,
    fnames: Union[str, List[str]],
    num_lines_sniffed: int,
    quotechar: str,
    sep: str,
    skip: int,
    colnames: Optional[List[str]],
    conn: Optional[Connection],
) -> None: ...


def _read_csv(
    type: CSVCmdType,
    name: str,
    fnames: Union[str, List[str]],
    num_lines_sniffed: int,
    quotechar: str,
    sep: str,
    skip: int,
    colnames: Optional[List[str]],
    conn: Optional[Connection] = None,
) -> Union[str, None]:
    conn = conn or Connection()

    if not isinstance(fnames, list):
        fnames = [fnames]

    cmd: Dict[str, Any] = {}

    cmd["name_"] = name
    cmd["type_"] = f"Database.{type.value}_csv"

    cmd["fnames_"] = _retrieve_urls_for_engine(fnames)
    cmd["num_lines_sniffed_"] = num_lines_sniffed
    cmd["quotechar_"] = quotechar
    cmd["sep_"] = sep
    cmd["skip_"] = skip
    cmd["conn_id_"] = conn.conn_id

    if colnames is not None:
        cmd["colnames_"] = colnames

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.handle_engine_exception(msg)
        if type == CSVCmdType.SNIFF:
            return comm.recv_string(sock)
