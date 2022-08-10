# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Executes an SQL query on the database and returns the result as
a pandas dataframe.
"""

from typing import Any, Dict, Optional

import pandas as pd  # type: ignore

import getml.communication as comm

from .connection import Connection


def get(query: str, conn: Optional[Connection] = None):
    """
    Executes an SQL query on the database and returns the result as
    a pandas dataframe.

    Args:
        query (str):
            The SQL query to be executed.

        conn (:class:`~getml.database.Connection`, optional):
            The database connection to be used.
            If you don't explicitly pass a connection,
            the engine will use the default connection.
    """

    conn = conn or Connection()

    cmd: Dict[str, Any] = {}

    cmd["name_"] = conn.conn_id
    cmd["type_"] = "Database.get"

    with comm.send_and_get_socket(cmd) as sock:
        comm.send_string(sock, query)
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.engine_exception_handler(msg)
        json_str = comm.recv_string(sock)

    return pd.read_json(json_str)
