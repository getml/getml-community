# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Returns a list handles to all connections that are currently active on the
engine.
"""

from typing import Any, Dict, List

import json

import getml.communication as comm

from .connection import Connection


def list_connections() -> List[Connection]:
    """
    Returns a list handles to all connections
    that are currently active on the engine.
    """

    cmd: Dict[Any, str] = {}

    cmd["name_"] = ""
    cmd["type_"] = "Database.list_connections"

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            sock.close()
            comm.engine_exception_handler(msg)
        arr = json.loads(comm.recv_string(sock))

    return [Connection(elem) for elem in arr]
