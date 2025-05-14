# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains various helper functions related to the getML Engine.
"""

import json
import socket
from typing import Dict, List

import getml.communication as comm
from getml.communication import (
    _delete_project,
    _list_projects_impl,
    _set_project,
    _shutdown,
    _suspend_project,
)

# backward compatibility alias
from getml.communication import is_engine_alive as is_alive

# --------------------------------------------------------------------


def delete_project(name: str):
    """Deletes a project.

    Args:
        name:
            Name of your project.

    Note:
        All data and models contained in the project directory will be
        permanently lost.

    """
    _delete_project(name)


# -----------------------------------------------------------------------------


def list_projects() -> List[str]:
    """
    List all projects on the getML Engine.

    Returns:
            Lists the name of all the projects.
    """
    return _list_projects_impl(running_only=False)


# -----------------------------------------------------------------------------


def list_running_projects() -> List[str]:
    """
    List all projects on the getML Engine that are currently running.

    Returns:
        Lists the name of all the projects currently running.
    """
    return _list_projects_impl(running_only=True)


# -----------------------------------------------------------------------------


def set_project(name: str):
    """Creates a new project or loads an existing one.

    If there is no project called `name` present on the Engine, a new one will
    be created.

    Args:
           name: Name of the new project.
    """
    _set_project(name)


# -----------------------------------------------------------------------------


def shutdown():
    """Shuts down the getML Engine.

    Warning:
        All changes applied to the [`DataFrame`][getml.DataFrame]
        after calling their [`save`][getml.DataFrame.save]
        method will be lost.

    """
    _shutdown()


# --------------------------------------------------------------------


def suspend_project(name: str):
    """Suspends a project that is currently running.

    Args:
        name:
            Name of your project.
    """
    _suspend_project(name)


# -----------------------------------------------------------------------------
