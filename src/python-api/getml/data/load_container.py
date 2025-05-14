# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Loads a container."""

import json
from typing import Any, Dict

import getml.communication as comm
from getml.data.columns import _parse

from .container import Container
from .helpers2 import _load_view


def load_container(container_id: str) -> Container:
    """
    Loads a container and all associated data frames from disk.

    Args:
        container_id:
            The id of the container you would like to load.

    Returns:
        The container with the given id.
    """

    cmd: Dict[str, Any] = {}
    cmd["type_"] = "DataContainer.load"
    cmd["name_"] = container_id

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.handle_engine_exception(msg)
        json_str = comm.recv_string(sock)

    cmd = json.loads(json_str)

    population = _load_view(cmd["population_"]) if "population_" in cmd else None

    peripheral = {k: _load_view(v) for (k, v) in cmd["peripheral_"].items()}

    subsets = {k: _load_view(v) for (k, v) in cmd["subsets_"].items()}

    split = _parse(cmd["split_"]) if "split_" in cmd else None

    deep_copy = cmd["deep_copy_"]
    frozen_time = cmd["frozen_time_"] if "frozen_time_" in cmd else None
    last_change = cmd["last_change_"]

    container = Container(
        population=population, peripheral=peripheral, deep_copy=deep_copy, **subsets
    )

    container._id = container_id
    container._frozen_time = frozen_time
    container._split = split
    container._last_change = last_change

    return container
