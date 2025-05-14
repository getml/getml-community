# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Copies a table from one database connection to another.
"""

from typing import Any, Dict, Optional

import getml.communication as comm

from .connection import Connection


def copy_table(
    source_conn: Connection,
    target_conn: Connection,
    source_table: str,
    target_table: Optional[str] = None,
):
    """
    Copies a table from one database connection to another.

    Args:
        source_conn:
            The database connection to be copied from.

        target_conn:
            The database connection to be copied to.

        source_table:
            The name of the table in the source connection.

        target_table:
            The name of the table in the target
            connection. If you do not explicitly pass a target_table, the
            name will be identical to the source_table.

    ??? example
        A frequent use case for this function is to copy data from a data source into
        sqlite. This is a good idea, because sqlite is faster than most standard,
        ACID-compliant databases, and also you want to avoid messing up a productive
        environment.

        It is important to explicitly pass conn_id, otherwise the source connection
        will be closed
        when you create the target connection. What you pass as conn_id is entirely
        up to you,
        as long as the conn_id of the source and the target are different. It can
        be any name you see fit.

        ```python
        source_conn = getml.database.connect_odbc(
            "MY-SERVER-NAME", conn_id="MY-SOURCE")

        target_conn = getml.database.connect_sqlite3(
            "MY-SQLITE.db", conn_id="MY-TARGET")

        data.database.copy_table(
                source_conn=source_conn,
                target_conn=target_conn,
                source_table="MY-TABLE"
        )
        ```

    """

    # -------------------------------------------

    target_table = target_table or source_table

    # -------------------------------------------

    cmd: Dict[str, Any] = {}

    cmd["name_"] = ""
    cmd["type_"] = "Database.copy_table"

    cmd["source_conn_id_"] = source_conn.conn_id
    cmd["target_conn_id_"] = target_conn.conn_id
    cmd["source_table_"] = source_table
    cmd["target_table_"] = target_table

    # -------------------------------------------

    comm.send(cmd)
