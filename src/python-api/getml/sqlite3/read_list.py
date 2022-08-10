# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Reads data into an sqlite3 table.
"""

import sqlite3

from .helpers import _get_num_columns, _log


def read_list(conn, table_name, data):
    """
    Reads data into an sqlite3 table.

    Args:
        conn:
            A sqlite3 connection created by :func:`~getml.sqlite3.connect`.

        table_name (str):
            The name of the table to write to.

        data (List[List[Object]]):
            The data to insert into the table.
            Every list represents one row to be read into the table.
    """

    # ------------------------------------------------------------

    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("'conn' must be an sqlite3.Connection object")

    if not isinstance(table_name, str):
        raise TypeError("'table_name' must be a string")

    if not isinstance(data, list):
        raise TypeError("'data' must be a list of lists")

    # ------------------------------------------------------------

    ncols = _get_num_columns(conn, table_name)
    old_length = len(data)
    data = [line for line in data if len(line) == ncols]
    placeholders = "(" + ",".join(["?"] * ncols) + ")"
    query = 'INSERT INTO "' + table_name + '" VALUES ' + placeholders
    conn.executemany(query, data)
    conn.commit()
    _log(
        "Read "
        + str(len(data))
        + " lines. "
        + str(old_length - len(data))
        + " invalid lines."
    )
