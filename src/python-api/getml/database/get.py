# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Executes an SQL query on the database and returns the result as
a pandas dataframe.
"""

import io
from typing import Any, Dict, Optional

import pandas as pd

import getml.communication as comm

from .connection import Connection


def get(query: str, conn: Optional[Connection] = None) -> pd.DataFrame:
    """
    Executes an SQL query on the database and returns the result as
    a pandas dataframe.

    Args:
        query:
            The SQL query to be executed.

        conn:
            The database connection to be used.
            If you don't explicitly pass a connection,
            the Engine will use the default connection.

    Returns:
        The result of the query as a pandas dataframe.
    """

    conn = conn or Connection()

    cmd: Dict[str, Any] = {}

    cmd["name_"] = conn.conn_id
    cmd["type_"] = "Database.get"

    with comm.send_and_get_socket(cmd) as sock:
        comm.send_string(sock, query)
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.handle_engine_exception(msg)
        json_str = comm.recv_string(sock)

    return pd.read_json(io.StringIO(json_str))
