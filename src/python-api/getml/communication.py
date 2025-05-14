# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Handles the communication for between engine, monitor and python API.
"""

from __future__ import annotations

import json
import numbers
import pathlib
import socket
import sys
from inspect import cleandoc
from os import PathLike
from time import sleep
from types import TracebackType
from typing import (
    Any,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Type,
    Union,
)

import numpy as np
from rich import print
from rich.progress import TaskID

import getml
from getml.events import (
    DispatcherEventEmitter,
    LogMessageEventParser,
    PoolEventDispatcher,
)
from getml.events.types import (
    EventContext,
    EventEmitter,
    EventParser,
    EventSource,
)
from getml.exceptions import handle_engine_exception
from getml.helpers import _is_iterable_not_str
from getml.version import __version__


class LogStreamListener:
    """
    Listens to raw log streams and emits structured events to a queue.
    """

    def __init__(
        self,
        parser: EventParser,
        emitter: EventEmitter,
    ):
        self.parser = parser
        self.emitter = emitter

    def __enter__(self) -> LogStreamListener:
        self.emitter.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ):
        if exc_value:
            self.emitter.__exit__(exc_type, exc_value, traceback)
            raise exc_value

        self.emitter.__exit__(exc_type, exc_value, traceback)

    def listen(self, socket: socket.socket, exit_on: Callable[[str], bool]) -> str:
        """
        Listen to a log stream on socket and emit events.
        """

        while True:
            message = recv_string(socket)

            events = self.parser.parse(message)
            if events:
                self.emitter.emit(events)

            if exit_on(exit_status := message):
                return exit_status


# --------------------------------------------------------------------

port = 1708
"""
The default starting port for getML Engines. The port is automatically set
according to the worker Engine process spawned when setting a project.
The Monitor (app) automatically looks up a free port (starting from 1708).
Setting the port here has no effect.
"""

tcp_port = 1711
"""
The TCP port of the getML Monitor (app) that schedules Engine workers
"""

# --------------------------------------------------------------------

ENGINE_CANNOT_CONNECT_ERROR_MSG_TEMPLATE = cleandoc(
    """
    Cannot reach the getML Engine. Please make sure you have set a project.

    To set: `getml.engine.set_project(...)`

    Available projects:
    {projects}
    """
)

ENGINE_API_VERSION_MISMATCH_ERROR_MSG_TEMPLATE = cleandoc(
    """
    The version of the getML API does not match the version of the getML Engine.

    API version: {api_version}
    Engine version: {engine_version}
    """
)

MONITOR_CANNOT_CONNECT_ERROR_MSG = cleandoc(
    """
    Cannot reach the getML Monitor. Please make sure the getML app is running.
    """
)

GETML_SEP = "$GETML_SEP"

SEP_SIZE = np.uint64(10)

# --------------------------------------------------------------------


def is_monitor_alive() -> bool:
    """
    Checks if the getML Monitor is running.

    Returns:
        `True` if the getML Monitor is running and ready to accept commands and
        `False` otherwise.
    """

    cmd = {
        "type_": "isalive",
        "body_": "",
    }

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect(("localhost", tcp_port))
        except ConnectionRefusedError:
            return False
        else:
            send_string(sock, json.dumps(cmd))
            return recv_string(sock) == "yes"


# --------------------------------------------------------------------


def is_engine_alive() -> bool:
    """
    Checks if an instance of the getML Engine is running.

    Returns:
        `True` if the getML Engine is running and ready to accept commands and
        `False` otherwise.

    """

    # no engine without monitor/cli
    if not is_monitor_alive():
        return False

    if not _list_projects_impl(running_only=True):
        return False

    try:
        with send_and_get_socket({"type_": "is_alive"}):
            return True
    except ConnectionRefusedError:
        return False


# -----------------------------------------------------------------------------

# --------------------------------------------------------------------


class _GetmlEncoder(json.JSONEncoder):
    """Enables a custom serialization of the getML classes."""

    def default(self, obj):  # type: ignore
        """Checks for the particular type of the provided object and
        deserializes it into an escaped string.

        To ensure the getML can handle all keys, we have to add a
        trailing underscore.

        Args:
            obj: Any of the classes defined in the getml package.

        Returns:
            string:
                Encoded JSON.

        Examples:
            Create a :class:`~getml.predictors.LinearRegression`,
            serialize it, and deserialize it again.

            ```python
                p = getml.predictors.LinearRegression()
                p_serialized = json.dumps(
                    p, cls = getml.communication._GetmlEncoder)
                p2 = json.loads(
                    p_serialized,
                    object_hook=getml.helpers.predictors._decode_predictor
                )
                p == p2
            ```
        """

        if hasattr(obj, "_getml_deserialize"):
            return obj._getml_deserialize()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if _is_iterable_not_str(obj):
            return list(obj)

        return json.JSONEncoder.default(self, obj)


# --------------------------------------------------------------------


def _make_error_msg() -> str:
    if not is_monitor_alive():
        return MONITOR_CANNOT_CONNECT_ERROR_MSG
    else:
        msg = ENGINE_CANNOT_CONNECT_ERROR_MSG_TEMPLATE.format(
            projects="\n".join(_list_projects_impl(running_only=False))
        )
        return msg


# --------------------------------------------------------------------


def recv_data(sock: socket.socket, size: Union[numbers.Real, int]) -> bytes:
    """Receives data (of any type) sent by the getML Engine.

    Args:
        sock: The socket to receive the data from.

        size: The size of the data to receive.

    Returns:
        The received data.
    """

    _, peer_port = sock.getpeername()

    if peer_port != tcp_port and not is_engine_alive():
        raise ConnectionRefusedError(_make_error_msg())

    data = []

    bytes_received = 0

    max_chunk_size = 2048

    while bytes_received < size:
        current_chunk_size = int(min(size - bytes_received, max_chunk_size))

        chunk = sock.recv(current_chunk_size)

        if not chunk:
            raise OSError(
                """The getML Engine died unexpectedly.
                    If this wasn't done on purpose, please get in contact
                    with our support or file a bug report."""
            )

        data.append(chunk)

        bytes_received += len(chunk)

    return b"".join(data)


# --------------------------------------------------------------------


def recv_float_matrix(sock: socket.socket) -> np.ndarray:
    """
    Receives a matrix (type np.float64) from the getML Engine.

    Args:
        sock: The socket to receive the matrix from.

    Returns:
        The received matrix.
    """

    if not isinstance(sock, socket.socket):
        raise TypeError("'sock' must be a socket.")

    # By default, numeric data sent over the socket is big endian,
    # also referred to as network-byte-order!
    if sys.byteorder == "little":
        shape_str = recv_data(sock, np.dtype(np.int32).itemsize * 2)

        shape = np.frombuffer(shape_str, dtype=np.int32).byteswap().astype(np.uint64)

        size = shape[0] * shape[1] * np.uint64(np.dtype(np.float64).itemsize)

        matrix = recv_data(sock, size)

        matrix = np.frombuffer(matrix, dtype=np.float64).byteswap()

        matrix = matrix.reshape(shape[0], shape[1])

    else:
        shape_str = recv_data(sock, np.dtype(np.int32).itemsize * 2)

        shape = np.frombuffer(shape_str, dtype=np.int32).astype(np.uint64)

        size = shape[0] * shape[1] * np.uint64(np.dtype(np.float64).itemsize)

        matrix = recv_data(sock, size)

        matrix = np.frombuffer(matrix, dtype=np.float64)

        matrix = matrix.reshape(shape[0], shape[1])

    return matrix


# --------------------------------------------------------------------


def recv_bytes(sock: socket.socket) -> bytes:
    """
    Receives bytes over a socket.
    """

    if not isinstance(sock, socket.socket):
        raise TypeError("'sock' must be a socket.")

    size_str = recv_data(sock, np.dtype(np.uint64).itemsize)

    size = (
        np.frombuffer(size_str, dtype=np.uint64).byteswap()[0]
        if sys.byteorder == "little"
        else np.frombuffer(size_str, dtype=np.uint64)[0]
    )

    return recv_data(sock, size)


# --------------------------------------------------------------------


def recv_string(sock: socket.socket) -> str:
    """
    Receives a string from the getML Engine
    (an actual string, not a bytestring).

    Args:
        sock: The socket to receive the string from.

    Returns:
        The received string.
    """

    # By default, numeric data sent over the socket is big endian,
    # also referred to as network-byte-order!
    if sys.byteorder == "little":
        size_str = recv_data(sock, np.dtype(np.int32).itemsize)

        size = np.frombuffer(size_str, dtype=np.int32).byteswap()[0]

    else:
        size_str = recv_data(sock, np.dtype(np.int32).itemsize)

        size = np.frombuffer(size_str, dtype=np.int32)[0]

    data = recv_data(sock, size)

    return data.decode(errors="ignore")


# --------------------------------------------------------------------


def recv_string_column(sock: socket.socket) -> np.ndarray:
    """
    Receives a column of type string from the getML Engine

    Args:
        sock: The socket to receive the column from.

    Returns:
        A numpy array containing the column.
    """

    if not isinstance(sock, socket.socket):
        raise TypeError("'sock' must be a socket.")

    nbytes_str = recv_data(sock, np.dtype(np.uint64).itemsize)

    if sys.byteorder == "little":
        nbytes = np.frombuffer(nbytes_str, dtype=np.uint64).byteswap()[0]
    else:
        nbytes = np.frombuffer(nbytes_str, dtype=np.uint64)[0]

    col = (
        recv_data(sock, nbytes).decode(errors="ignore").split(GETML_SEP)
        if nbytes != 0
        else []
    )

    col = np.asarray(col, dtype=object)

    return col.reshape(len(col), 1)


# --------------------------------------------------------------------


class _Issue(NamedTuple):
    """
    Describes a single warning generated
    by the check method of the pipeline.
    """

    message: str
    label: str
    warning_type: str


# --------------------------------------------------------------------


def recv_issues(sock: socket.socket) -> List[_Issue]:
    """
    Receives a set of warnings to raise from the getML Engine.

    Args:
        sock: The socket to receive the warnings from.

    Returns:
        A list of warnings.
    """

    if not isinstance(sock, socket.socket):
        raise TypeError("'sock' must be a socket.")

    json_str = recv_string(sock)

    if json_str[0] != "{":
        raise ValueError(json_str)

    json_obj = json.loads(json_str)

    all_warnings = json_obj["warnings_"]

    return [
        _Issue(
            message=w["message_"], label=w["label_"], warning_type=w["warning_type_"]
        )
        for w in all_warnings
    ]


# --------------------------------------------------------------------


def send(cmd: Dict[str, Any]) -> None:
    """Sends a command to the getML Engine and closes the established
    connection.

    Creates a socket and sends a command to the getML Engine using the
    module-wide variable [`port`][getml.communication.port].

    A message (string) from the `socket.socket` will be
    received using [`recv_string`][getml.communication.recv_string] and the socket will
    be closed. If the message is "Success!", everything works
    properly. Else, an Exception will be thrown containing the
    message.

    In case another message is supposed to be sent by the Engine,
    [`send_and_get_socket`][getml.communication.send_and_get_socket] has to be used
    and the calling function must handle the message itself!

    Please be very careful when altering the routing/calling behavior
    of the socket communication! The Engine is quite sensitive and might freeze.

    Args:
        cmd: A dictionary specifying the command the Engine is
            supposed to execute. It _must_ contain at least two string
            values with the corresponding keys being named "name_" and
            "type_".
    """

    if not isinstance(cmd, dict):
        raise TypeError("'cmd' must be a dict.")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(("localhost", port))
    except ConnectionRefusedError:
        raise ConnectionRefusedError(_make_error_msg())

    # Use a custom encoder which knows how to handle the classes
    # defined in the getml package.
    msg = json.dumps(cmd, cls=_GetmlEncoder)

    send_string(sock, msg)

    # The calling function does not want to further use the
    # socket. Therefore, it will be checked here whether the
    # request was successful and closed.
    msg = recv_string(sock)

    sock.close()

    if msg != "Success!":
        handle_engine_exception(msg)


# --------------------------------------------------------------------


def send_and_get_socket(
    cmd: Dict[str, Any], specific_port: Optional[int] = None
) -> socket.socket:
    """Sends a command to the getML Engine and returns the established
    connection.

    Creates a socket and sends a command to the getML Engine using the
    module-wide variable [`port`][getml.communication.port].

    The function will return the socket it opened and the calling
    function is free to receive whatever data is desires over it. But
    the latter also has to take care of closing the socket afterwards
    itself!

    Please be very careful when altering the routing/calling behavior
    of the socket communication! The Engine is quite sensitive and might freeze.
    Especially implemented handling of
    socket sessions (their passing from function to function) must not
    be altered or separated in distinct calls to the
    [`send`][getml.communication.send] function! Some commands have
    to be sent via the same socket or the Engine will not be able to
    handle them and might block.

    Args:
        cmd: A dictionary specifying the command the Engine is
            supposed to execute. It _must_ contain at least two string
            values with the corresponding keys being named "name_" and
            "type_"."

    Returns:
        A socket which, using the Python API, can communicate with the getML Engine.
    """

    if not isinstance(cmd, dict):
        raise TypeError("'cmd' must be a dict.")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(
            ("localhost", specific_port if specific_port is not None else port)
        )
    except ConnectionRefusedError:
        raise ConnectionRefusedError(_make_error_msg())

    # Use a custom encoder which knows how to handle the classes
    # defined in the getml package.
    msg = json.dumps(cmd, cls=_GetmlEncoder)

    send_string(sock, msg)

    return sock


# --------------------------------------------------------------------


def send_bytes(sock: socket.socket, data: bytes):
    """
    Sends bytes over a socket.
    """

    if not isinstance(sock, socket.socket):
        raise TypeError("'sock' must be a socket.")

    if not isinstance(data, bytes):
        raise TypeError("'data' must be bytes.")

    size = len(data)

    # By default, numeric data sent over the socket is big endian,
    # also referred to as network-byte-order!
    if sys.byteorder == "little":
        sock.sendall(np.asarray([size]).astype(np.int32).byteswap().tobytes())
    else:
        sock.sendall(np.asarray([size]).astype(np.int32).tobytes())

    sock.sendall(data)


# --------------------------------------------------------------------


def send_string(sock: socket.socket, string: str):
    """
    Sends a string over a socket
    (an actual string, not a bytestring).

    Args:
        sock: The socket to send the string to.

        string: The string to send.
    """

    if not isinstance(sock, socket.socket):
        raise TypeError("'sock' must be a socket.")

    if not isinstance(string, str):
        raise TypeError("'string' must be a str.")

    encoded = string.encode("utf-8")

    send_bytes(sock, encoded)


# --------------------------------------------------------------------


def log(sock: socket.socket, extra: Optional[Dict[str, Any]] = None) -> str:
    """
    Handles logs sent by engine or monitor.

    Args:
        sock: The socket to receive the logs from.
    """

    if extra is None:
        extra = {}

    peer_port = sock.getpeername()[1]

    event_source = EventSource.ENGINE if peer_port == port else EventSource.MONITOR

    parser = LogMessageEventParser(
        context=EventContext(cmd=extra.get("cmd", {}), source=event_source)
    )
    dispatcher = PoolEventDispatcher()
    emitter = DispatcherEventEmitter(dispatcher)

    def is_status_message(message: str) -> bool:
        return not message.startswith("log: ")

    with LogStreamListener(parser, emitter) as listener:
        status_message = listener.listen(socket=sock, exit_on=is_status_message)
        return status_message


# --------------------------------------------------------------------


def _delete_project(name: str):
    if not isinstance(name, str):
        raise TypeError("'name' must be of type str")

    _delete_project_with_retry(name, retries=10, delay=0.2)


# --------------------------------------------------------------------


def _delete_project_with_retry(name: str, retries: int, delay: float):
    """
    Attempt to delete the project, retrying `retries` times if necessary.
    """
    if name in _list_projects_impl(running_only=True):
        _suspend_project(name)

    for _ in range(retries):
        if name not in _list_projects_impl(running_only=False):
            return

        cmd: Dict[str, Any] = {}
        cmd["type_"] = "deleteproject"
        cmd["body_"] = name

        with send_and_get_socket(cmd, tcp_port) as sock:
            msg = recv_string(sock)
            if msg != "Success!":
                handle_engine_exception(msg)

        sleep(delay)

    raise OSError(f"Failed to delete project '{name}' after {retries} attempts.")


# --------------------------------------------------------------------


def _get_project_name() -> str:
    cmd: Dict[str, Any] = {}
    cmd["type_"] = "project_name"
    cmd["name_"] = ""

    with send_and_get_socket(cmd) as sock:
        pname = recv_string(sock)

    return pname


# --------------------------------------------------------------------


def _load_project(bundle: Union[PathLike, str], name: Optional[str] = None):
    name = name or ""

    if not isinstance(bundle, (str, PathLike)):
        raise TypeError("'bundle' must be of type str")

    if not isinstance(name, str):
        raise TypeError("'name' must be of type str")

    with open(bundle, "rb") as f:
        data = f.read()

    cmd: Dict[str, Any] = {}
    cmd["type_"] = "loadprojectbundle"
    cmd["body_"] = dict(name_=name)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.connect(("localhost", tcp_port))
    except ConnectionRefusedError:
        raise ConnectionRefusedError(_make_error_msg())

    msg = json.dumps(cmd, cls=_GetmlEncoder)

    send_string(sock, msg)

    send_bytes(sock, data)

    msg = log(sock, extra={"cmd": cmd})

    if msg != "Success!":
        handle_engine_exception(msg)

    global port  # noqa:PLW0603
    port = int(recv_string(sock))

    proj_name = _get_project_name()

    print(f"Loaded {bundle!r} as {proj_name!r}. Connected to project {proj_name!r}.")


# --------------------------------------------------------------------


def _list_projects_impl(running_only: bool) -> List[str]:
    cmd = dict()
    cmd["type_"] = "listallprojects"
    cmd["body_"] = ""

    if running_only:
        cmd["type_"] = "listrunningprojects"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.connect(("localhost", tcp_port))
    except ConnectionRefusedError:
        raise ConnectionRefusedError(_make_error_msg())

    msg = json.dumps(cmd, cls=_GetmlEncoder)

    send_string(sock, msg)

    msg = recv_string(sock)

    if msg != "Success!":
        handle_engine_exception(msg)

    json_str = recv_string(sock)

    sock.close()

    return json.loads(json_str)["projects"]


# --------------------------------------------------------------------


def _monitor_url() -> Optional[str]:
    cmd: Dict[str, str] = {}

    cmd["type_"] = "monitor_url"
    cmd["name_"] = ""

    with send_and_get_socket(cmd) as sock:
        return recv_string(sock) or None


# --------------------------------------------------------------------


def _check_version():
    cmd: Dict[str, Any] = {}
    cmd["type_"] = "getversion"
    cmd["body_"] = ""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect(("localhost", tcp_port))
        except ConnectionRefusedError:
            raise ConnectionRefusedError(_make_error_msg())
        else:
            send_string(sock, json.dumps(cmd, cls=_GetmlEncoder))
            engine_version = recv_string(sock)

    if engine_version != __version__:
        raise ValueError(
            ENGINE_API_VERSION_MISMATCH_ERROR_MSG_TEMPLATE.format(
                api_version=__version__, engine_version=engine_version
            )
        )


# --------------------------------------------------------------------


def _save_project(
    name: str,
    file_name: Optional[Union[PathLike, str]] = None,
    replace: bool = False,
):
    if not isinstance(name, str):
        raise TypeError("'name' must be of type str")

    if not isinstance(file_name, (str, PathLike)) and file_name is not None:
        raise TypeError("'file_name' must be a str or PathLike")

    if file_name is None:
        file_name = name

    target_path = pathlib.Path(file_name).resolve()
    if target_path.suffix == "":
        target_path = target_path.with_suffix(".getml")

    if target_path.exists() and not replace:
        raise FileExistsError(
            f"The file '{target_path}' already exists. "
            "Use 'replace=True' to overwrite it."
        )

    cmd: Dict[str, Any] = {}
    cmd["type_"] = "bundleproject"
    cmd["body_"] = {"name_": name}
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.connect(("localhost", tcp_port))
    except ConnectionRefusedError:
        raise ConnectionRefusedError(_make_error_msg())

    msg = json.dumps(cmd, cls=_GetmlEncoder)

    send_string(sock, msg)

    msg = recv_string(sock)

    if msg != "Success!":
        handle_engine_exception(msg)

    bundle = recv_bytes(sock)

    with open(target_path, "wb") as f:
        f.write(bundle)

    print(f"Saved project {name} to '{target_path}'")


# --------------------------------------------------------------------


def _project_url() -> Optional[str]:
    url = _monitor_url()
    return url + "listprojects/" + _get_project_name() + "/" if url else None


# --------------------------------------------------------------------


def _set_project(name: str, restart: bool = False):
    if not isinstance(name, str):
        raise TypeError("'name' must be of type str")

    if not is_monitor_alive():
        getml.engine.launch(quiet=True)

    _check_version()

    cmd: Dict[str, Any] = {}
    cmd["type_"] = "setproject"
    cmd["body_"] = name

    if restart:
        cmd["type_"] = "restartproject"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.connect(("localhost", tcp_port))
    except ConnectionRefusedError:
        raise ConnectionRefusedError(_make_error_msg())

    msg = json.dumps(cmd, cls=_GetmlEncoder)

    send_string(sock, msg)

    msg = log(sock, extra={"cmd": cmd})

    if msg != "Success!":
        handle_engine_exception(msg)

    global port  # noqa:PLW0603

    port = int(recv_string(sock))

    print(f"Connected to project {name!r}.")

    if _project_url():
        print(_project_url())


# --------------------------------------------------------------------


def _shutdown():
    cmd: Dict[str, Any] = {}
    cmd["type_"] = "shutdownlocal"
    cmd["body_"] = ""

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.connect(("localhost", tcp_port))
    except ConnectionRefusedError:
        raise ConnectionRefusedError(_make_error_msg())

    msg = json.dumps(cmd, cls=_GetmlEncoder)

    print("Shutting down the getML Engine...", end=" ")

    send_string(sock, msg)

    sock.close()

    while True:
        try:
            with socket.create_connection(("localhost", tcp_port), timeout=5.0):
                pass
        except:  # noqa: E722
            print("Done.")
            return


# --------------------------------------------------------------------


def _suspend_project(name: str):
    if not isinstance(name, str):
        raise TypeError("'name' must be of type str")

    cmd: Dict[str, Any] = {}
    cmd["type_"] = "suspendproject"
    cmd["body_"] = name

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.connect(("localhost", tcp_port))
    except ConnectionRefusedError:
        raise ConnectionRefusedError(_make_error_msg())

    msg = json.dumps(cmd, cls=_GetmlEncoder)

    send_string(sock, msg)

    msg = recv_string(sock)

    if msg != "Success!":
        handle_engine_exception(msg)

    sock.close()
