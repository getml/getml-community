# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Lists all tables and views currently held in the database.
"""

import json
from typing import Any, Dict, List, Optional

import getml.communication as comm

from .connection import Connection


def list_tables(conn: Optional[Connection] = None) -> List[str]:
    """
    Lists all tables and views currently held in the database.

    Args:
        conn (:class:`~getml.database.Connection`, optional):
            The database connection to be used.
            If you don't explicitly pass a connection,
            the engine will use the default connection.
    """

    conn = conn or Connection()

    cmd: Dict[str, Any] = {}

    cmd["name_"] = conn.conn_id
    cmd["type_"] = "Database.list_tables"

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            sock.close()
            comm.engine_exception_handler(msg)
        return json.loads(comm.recv_string(sock))
