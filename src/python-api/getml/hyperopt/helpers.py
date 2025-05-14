# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Lists all hyperparameter optimization objects present in the Engine."""

import json
from typing import Any, Dict, List

import getml.communication as comm

# --------------------------------------------------------------------


def list_hyperopts() -> List[str]:
    """Lists all hyperparameter optimization objects present in the Engine.

    Note that this function only lists hyperopts which are part of the
    current project. See [`set_project`][getml.engine.set_project] for
    changing projects.

    To subsequently load one of them, use
    [`load_hyperopt`][getml.hyperopt.load_hyperopt.load_hyperopt].

    Returns:
        list containing the names of all hyperopts.

    """

    cmd: Dict[str, Any] = {}
    cmd["type_"] = "list_hyperopts"
    cmd["name_"] = ""

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.handle_engine_exception(msg)
        json_str = comm.recv_string(sock)

    return json.loads(json_str)["names"]


# --------------------------------------------------------------------


def exists(name: str) -> bool:
    """Determines whether a hyperopt exists.

    Args:
        name: The name of the hyperopt.

    Returns:
        A boolean indicating whether a hyperopt named 'name' exists.
    """
    if not isinstance(name, str):
        raise TypeError("'name' must be of type str")

    return name in list_hyperopts()


# --------------------------------------------------------------------


def delete(name: str) -> None:
    """
    If a hyperopt named 'name' exists, it is deleted.

    Args:
        name: The name of the hyperopt.
    """

    if not exists(name):
        return

    cmd: Dict[str, Any] = {}

    cmd["type_"] = "Hyperopt.delete"
    cmd["name_"] = name

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.handle_engine_exception(msg)
