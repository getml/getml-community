# Copyright 2025 Code17 GmbH
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
        conn:
            The database connection to be used.
            If you don't explicitly pass a connection,
            the Engine will use the default connection.

    Returns:
        A list of strings containing the names of the tables and views.
    """

    conn = conn or Connection()

    cmd: Dict[str, Any] = {}

    cmd["name_"] = conn.conn_id
    cmd["type_"] = "Database.list_tables"

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.handle_engine_exception(msg)
        return json.loads(comm.recv_string(sock))
