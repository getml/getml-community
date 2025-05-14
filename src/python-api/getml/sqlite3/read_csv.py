# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains utility functions for reading CSV files
into sqlite3.
"""

from __future__ import annotations

import csv
import sqlite3
from typing import List, Optional, Union

from getml.data.helpers import _is_non_empty_typed_list, _is_typed_list

from .helpers import _create_table, _log
from .read_list import read_list
from .sniff_csv import sniff_csv

# ----------------------------------------------------------------------------


def _read_csv_file(fname, sep, quotechar, header, skip=0):
    with open(fname, newline="\n") as csvfile:
        reader = csv.reader(csvfile, delimiter=sep, quotechar=quotechar)
        if header:
            return list(reader)[skip + 1 :]
        return list(reader)[skip:]


# ----------------------------------------------------------------------------


def read_csv(
    conn: sqlite3.Connection,
    fnames: Union[str, List[str]],
    table_name: str,
    header: bool = True,
    if_exists: str = "append",
    quotechar: str = '"',
    sep: str = ",",
    skip: int = 0,
    colnames: Optional[List[str]] = None,
) -> None:
    """
    Reads a list of CSV files and writes them into an sqlite3 table.

    Args:
        conn:
            A sqlite3 connection created by [`connect`][getml.sqlite3.connect.connect].

        fnames:
            The names of the CSV files.

        table_name:
            The name of the table to write to.

        header:
            Whether the csv file contains a header. If True, the first line
            is skipped and column names are inferred accordingly.

        quotechar:
            The string escape character.

        if_exists:
            How to behave if the table already exists:

            - 'fail': Raise a ValueError.
            - 'replace': Drop the table before inserting new values.
            - 'append': Insert new values to the existing table.

        sep:
            The field separator.

        skip:
            The number of lines to skip (before a possible header)

        colnames:
            The first line of a CSV file
            usually contains the column names. When this is not the case, you can
            explicitly pass them. If you pass colnames, it is assumed that the
            CSV files do not contain a header, thus overriding the 'header' variable.
    """
    # ------------------------------------------------------------

    if not isinstance(fnames, list):
        fnames = [fnames]

    # ------------------------------------------------------------

    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("'conn' must be an sqlite3.Connection object")

    if not _is_non_empty_typed_list(fnames, str):
        raise TypeError("'fnames' must be a string or a non-empty list of strings")

    if not isinstance(table_name, str):
        raise TypeError("'table_name' must be a string")

    if not isinstance(header, bool):
        raise TypeError("'header' must be a bool")

    if not isinstance(quotechar, str):
        raise TypeError("'quotechar' must be a str")

    if not isinstance(if_exists, str):
        raise TypeError("'if_exists' must be a str")

    if not isinstance(sep, str):
        raise TypeError("'sep' must be a str")

    if not isinstance(skip, int):
        raise TypeError("'skip' must be an int")

    if colnames is not None and not _is_typed_list(colnames, str):
        raise TypeError("'colnames' must be a list of strings or None")

    # ------------------------------------------------------------

    schema = sniff_csv(
        fnames=fnames,
        table_name=table_name,
        header=header,
        quotechar=quotechar,
        sep=sep,
        skip=skip,
        colnames=colnames,
    )

    _create_table(conn, table_name, schema, if_exists)

    for fname in fnames:
        _log("Loading '" + fname + "' into '" + table_name + "'...")
        data = _read_csv_file(fname, sep, quotechar, header and not colnames, skip)
        read_list(conn, table_name, data)
