# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Helper functions that depend on Pipeline.
"""

import json
from typing import Any, Dict, List

import getml.communication as comm
from getml.data import DataModel

from .pipeline import Pipeline

# --------------------------------------------------------------------


def _make_dummy(name: str) -> Pipeline:
    data_model = DataModel("dummy")
    pipeline = Pipeline(data_model=data_model)
    pipeline._id = name
    return pipeline


# --------------------------------------------------------------------


def _from_json(json_obj: Dict[str, Any]) -> Pipeline:
    pipe = _make_dummy("dummy")
    pipe._parse_json_obj(json_obj)
    return pipe


# --------------------------------------------------------------------


def _refresh_all() -> List[Pipeline]:
    cmd: Dict[str, Any] = {}

    cmd["type_"] = "Pipeline.refresh_all"
    cmd["name_"] = ""

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.handle_engine_exception(msg)
        json_str = comm.recv_string(sock)

    json_obj = json.loads(json_str)

    return [_from_json(obj) for obj in json_obj["pipelines"]]


# --------------------------------------------------------------------


def list_pipelines() -> List[str]:
    """Lists all pipelines present in the Engine.

    Note that this function only lists pipelines which are part of the
    current project. See [`set_project`][getml.engine.set_project] for
    changing projects and [`pipelines`][getml.pipeline] for more details about
    the lifecycles of the pipelines.

    To subsequently load one of them, use
    [`load`][getml.pipeline.load].

    Returns:
        List containing the names of all pipelines.
    """

    cmd: Dict[str, Any] = {}

    cmd["type_"] = "list_pipelines"
    cmd["name_"] = ""

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.handle_engine_exception(msg)
        json_str = comm.recv_string(sock)

    return json.loads(json_str)["names"]


# --------------------------------------------------------------------


def load(name: str) -> Pipeline:
    """Loads a pipeline from the getML Engine into Python.

    Args:
        name: The name of the pipeline to be loaded.

    Returns:
        Pipeline that is a handler for the pipeline signified by name.
    """

    return _make_dummy(name).refresh()


# --------------------------------------------------------------------


def exists(name: str) -> bool:
    """
    Returns true if a pipeline named 'name' exists.

    Args:
        name (str):
            Name of the pipeline.

    Returns:
            True if the pipeline exists, False otherwise.
    """
    if not isinstance(name, str):
        raise TypeError("'name' must be of type str")

    all_pipelines = list_pipelines()

    return name in all_pipelines


# --------------------------------------------------------------------


def delete(name: str) -> None:
    """
    If a pipeline named 'name' exists, it is deleted.

    Args:
        name:
            Name of the pipeline.
    """

    if not isinstance(name, str):
        raise TypeError("'name' must be of type str")

    if exists(name):
        _make_dummy(name).delete()


# --------------------------------------------------------------------
