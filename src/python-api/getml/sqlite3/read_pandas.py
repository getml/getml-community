# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains utility functions for reading a pandas DataFrame
into sqlite3.
"""

import numbers
import sqlite3

import pandas as pd  # type: ignore

from .helpers import _create_table, _get_colnames, _log
from .read_list import read_list
from .sniff_pandas import sniff_pandas


def read_pandas(conn, table_name, data_frame, if_exists="append"):
    """
    Loads a pandas.DataFrame into SQLite3.

    Args:
        conn:
            A sqlite3 connection created by :func:`~getml.sqlite3.connect`.

        table_name (str):
            The name of the table to write to.

        data_frame (pandas.DataFrame):
            The pandas.DataFrame to read
            into the table. The column names must match the column
            names of the target table in the SQLite3 database, but
            their order is not important.

        if_exists (str):
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
