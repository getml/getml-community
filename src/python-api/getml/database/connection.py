# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
A handle to a database connection on the getML Engine.
"""

import json
from typing import Any, Dict

import getml.communication as comm
from getml.utilities.formatting import _SignatureFormatter


class Connection:
    """
    A handle to a database connection on the getML Engine.

    Attributes:
        conn_id:
            The name you want to use to reference the connection.
            You can call it
            anything you want to. If a database
            connection with the same conn_id already exists, that connection
            will be removed automatically and the new connection will take its place.
            The default conn_id is "default", which refers to the default connection.
            If you do not explicitly pass a connection handle to any function that
            relates to a database, the default connection will be used automatically.
    """

    def __init__(self, conn_id: str = "default"):
        self.conn_id = conn_id

    def __repr__(self):
        return str(self)

    def __str__(self):
        cmd: Dict[str, Any] = {}

        cmd["name_"] = self.conn_id
        cmd["type_"] = "Database.describe_connection"

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)
            if msg != "Success!":
                comm.handle_engine_exception(msg)
            description = comm.recv_string(sock)

        json_obj = json.loads(description)

        json_obj["type"] = "Connection"

        sig = _SignatureFormatter(data=json_obj)

        return sig._format()
