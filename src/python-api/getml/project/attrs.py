# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Handles access to project-related information.
"""

from sys import modules
from typing import List, Optional

from rich import print

import getml.communication as comm
from getml.engine import is_engine_alive, is_monitor_alive, list_projects

from .containers import DataFrames, Hyperopts, Pipelines

_functions: List[str] = []

_props: List[str] = []


def __getattr__(key):
    if key in _functions:
        return getattr(modules.get(__name__), key)

    if key in _props:
        if is_engine_alive():
            return getattr(modules.get(__name__), "_" + key)()
        elif is_monitor_alive():
            projects = "\n".join(list_projects())
            msg = comm._make_error_msg()
            msg += "\n\nAvailable projects:\n\n"
            msg += projects
            print(msg)
            return None
        else:
            msg = comm._make_error_msg()
            print(msg)
            return None

    raise AttributeError(f"module 'getml.project' has no attribute {key}")


def module_function(func):
    _functions.append(func.__name__)
    return func


def module_prop(prop):
    _props.append(prop.__name__[1:])
    return prop


@module_prop
def _data_frames():
    return DataFrames()


@module_prop
def _hyperopts():
    return Hyperopts()


@module_prop
def _pipelines():
    return Pipelines()


@module_prop
def _name():
    return comm._get_project_name()


@module_function
def load(bundle: str, name: Optional[str] = None) -> None:
    """
    Loads a project from a bundle and connects to it.

    Args:
        bundle: The `.getml` bundle file to load.

        name: A name for the project contained in the bundle.
          If None, the name will be extracted from the bundle.
    """
    return comm._load_project(bundle, name)


@module_function
def delete() -> None:
    """
    Deletes the currently connected project. All related pipelines,
    data frames and hyperopts will be irretrievably deleted.
    """
    comm._delete_project(_name())


@module_function
def restart() -> None:
    """
    Suspends and then relaunches the currently connected project.
    This will kill all jobs currently running on that process.
    """
    comm._set_project(_name(), restart=True)


@module_function
def save(
    filename: Optional[str] = None,
    target_dir: Optional[str] = None,
    replace: bool = True,
) -> None:
    """
    Saves the currently connected project to disk.

    Args:
        filename: The name of the `.getml` bundle file

        target_dir: the directory to save the bundle to.
          If None, the current working directory is used.

        replace: Whether to replace an existing bundle.
    """
    return comm._save_project(_name(), filename, target_dir, replace)


@module_function
def suspend() -> None:
    """
    Suspends the currently connected project.
    """
    return comm._suspend_project(_name())


@module_function
def switch(name: str) -> None:
    """Creates a new project or loads an existing one.

    If there is no project called `name` present on the engine, a new one will
    be created. See the [User guide][project-management] for more
    information.

    Args:
        name: Name of the new project.
    """
    comm._set_project(name)


_all = _functions + _props
