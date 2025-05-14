# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Returns a table as a list of lists.
"""

from __future__ import annotations

import sqlite3
from typing import List, Tuple

# ----------------------------------------------------------------------------


def _handle_query(query):
    if "SELECT" in query:
        return query
    return 'SELECT * FROM "' + query + '";'


# ----------------------------------------------------------------------------


def to_list(conn: sqlite3.Connection, query: str) -> Tuple[List[str], List[list]]:
    """
    Transforms a query or table into a list of lists. Returns
    a tuple which contains the column names and the actual data.

    Args:
        conn:
            A sqlite3 connection created by [`connect`][getml.sqlite3.connect.connect].

        query:
            The query used to get the table. You can also
            pass the name of the table, in which case the entire
            table will be imported.

    Returns:
            The column names and the data as a list of lists.
    """
    # ------------------------------------------------------------

    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("'conn' must be an sqlite3.Connection object")

    if not isinstance(query, str):
        raise TypeError("'query' must be a str")

    # ------------------------------------------------------------

    query = _handle_query(query)
    cursor = conn.execute(query)
    colnames = [description[0] for description in cursor.description]
    data = cursor.fetchall()
    return colnames, data
