# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Drops a table from the database.
"""

from typing import Any, Dict, Optional

import getml.communication as comm

from .connection import Connection


def drop_table(name: str, conn: Optional[Connection] = None):
    """
    Drops a table from the database.

    Args:
        name:
            The table to be dropped.

        conn:
            The database connection to be used.
            If you don't explicitly pass a connection,
            the Engine will use the default connection.
    """

    # -------------------------------------------

    conn = conn or Connection()

    # -------------------------------------------

    cmd: Dict[str, Any] = {}

    cmd["name_"] = name
    cmd["type_"] = "Database.drop_table"
    cmd["conn_id_"] = conn.conn_id

    # -------------------------------------------

    comm.send(cmd)
