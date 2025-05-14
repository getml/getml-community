# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains utility functions for reading a pandas DataFrame
into sqlite3.
"""

from __future__ import annotations

import numbers
import sqlite3
from typing import Literal

import pandas as pd

from .helpers import _create_table, _get_colnames, _log
from .read_list import read_list
from .sniff_pandas import sniff_pandas


def read_pandas(
    conn: sqlite3.Connection,
    table_name: str,
    data_frame: pd.DataFrame,
    if_exists: Literal["fail", "replace", "append"] = "append",
) -> None:
    """
    Loads a pandas.DataFrame into SQLite3.

    Args:
        conn:
            A sqlite3 connection created by [`connect`][getml.sqlite3.connect.connect].

        table_name:
            The name of the table to write to.

        data_frame:
            The pandas.DataFrame to read
            into the table. The column names must match the column
            names of the target table in the SQLite3 database, but
            their order is not important.

        if_exists:
            How to behave if the table already exists:

            - 'fail': Raise a ValueError.
            - 'replace': Drop the table before inserting new values.
            - 'append': Insert new values into the existing table.
    """

    # ------------------------------------------------------------

    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("'conn' must be an sqlite3.Connection object")

    if not isinstance(table_name, str):
        raise TypeError("'table_name' must be a str")

    if not isinstance(data_frame, pd.DataFrame):
        raise TypeError("'data_frame' must be a pandas.DataFrame")

    if not isinstance(if_exists, str):
        raise TypeError("'if_exists' must be a str")

    # ------------------------------------------------------------

    _log("Loading pandas.DataFrame into '" + table_name + "'...")

    schema = sniff_pandas(table_name, data_frame)

    _create_table(conn, table_name, schema, if_exists)

    colnames = _get_colnames(conn, table_name)
    data = data_frame[colnames].values.tolist()
    data = [
        [
            field
            if isinstance(field, (numbers.Number, str)) or field is None
            else str(field)
            for field in row
        ]
        for row in data
    ]
    read_list(conn, table_name, data)
