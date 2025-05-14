# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Executes an SQL query on the database.
"""

from typing import Any, Dict, Optional

import getml.communication as comm

from .connection import Connection


def execute(query: str, conn: Optional[Connection] = None):
    """
    Executes an SQL query on the database.

    Please note that this is not meant to return results. If you want to
    get results, use `database.get()` instead.

    Args:
        query:
            The SQL query to be executed.

        conn:
            The database connection to be used.
            If you don't explicitly pass a connection,
            the Engine will use the default connection.
    """

    conn = conn or Connection()

    cmd: Dict[str, Any] = {}

    cmd["name_"] = conn.conn_id
    cmd["type_"] = "Database.execute"

    with comm.send_and_get_socket(cmd) as sock:
        comm.send_string(sock, query)
        msg = comm.recv_string(sock)

    if msg != "Success!":
        comm.handle_engine_exception(msg)
