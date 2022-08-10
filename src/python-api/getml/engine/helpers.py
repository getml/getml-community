# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains various helper functions related to the getML engine.
"""

import json
import socket
from typing import Dict

import getml.communication as comm
from getml.communication import (
    _delete_project,
    _list_projects_impl,
    _set_project,
    _shutdown,
    _suspend_project,
)

# --------------------------------------------------------------------


def delete_project(name):
    """Deletes a project.

    Args:
        name (str):
            Name of your project.

    Note:
        All data and models contained in the project directory will be
        permanently lost.

    """
    _delete_project(name)


# -----------------------------------------------------------------------------


def is_engine_alive():
    """Checks if the getML engine is running.

    Returns:
        bool:
            True if the getML engine is running and ready to accept
            commands and False otherwise.

    """

    cmd: Dict[str, str] = {}
    cmd["type_"] = "is_alive"
    cmd["name_"] = ""

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(("localhost", comm.port))
    except ConnectionRefusedError:
        return False

    comm.send_string(sock, json.dumps(cmd))

    sock.close()

    return True


# -----------------------------------------------------------------------------

# define compatability alias
is_alive = is_engine_alive


# -----------------------------------------------------------------------------


def list_projects():
    """
    List all projects on the getML engine.

    Returns:
        List[str]:
            Lists the name all of the projects.
    """
    return _list_projects_impl(running_only=False)


# -----------------------------------------------------------------------------


def list_running_projects():
    """
    List all projects on the getML engine that are currently running.

    Returns:
        List[str]: Lists the name all of the projects currently running.
    """
    return _list_projects_impl(running_only=True)


# -----------------------------------------------------------------------------


def set_project(name):
    """Creates a new project or loads an existing one.

    If there is no project called `name` present on the engine, a new one will
    be created.

    Args:
        name (str):
            Name of the new project.
    """
    _set_project(name)


# -----------------------------------------------------------------------------


def shutdown():
    """Shuts down the getML engine.

    Note:
        All changes applied to the :class:`~getml.DataFrame`
        after calling their :meth:`~getml.DataFrame.save`
        method will be lost.

    """
    _shutdown()


# --------------------------------------------------------------------


def suspend_project(name):
    """Suspends a project that is currently running.

    Args:
        name (str):
            Name of your project.
    """
    _suspend_project(name)


# -----------------------------------------------------------------------------
