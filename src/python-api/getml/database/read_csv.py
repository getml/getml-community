# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Reads a CSV file into the database.
"""

from typing import Any, Dict, List, Optional, Union

import getml.communication as comm

from .connection import Connection
from .helpers import _retrieve_urls


def read_csv(
    name: str,
    fnames: Union[str, List[str]],
    quotechar: str = '"',
    sep: str = ",",
    num_lines_read: int = 0,
    skip: int = 0,
    colnames: Optional[List[str]] = None,
    conn: Optional[Connection] = None,
):
    """
    Reads a CSV file into the database.

    Example:
        Let's assume you have two CSV files - *file1.csv* and
        *file2.csv* . You can import their data into the database
        using the following commands:

        >>> stmt = data.database.sniff_csv(
        ...         fnames=["file1.csv", "file2.csv"],
        ...         name="MY_TABLE",
        ...         sep=';'
        ... )
        >>>
        >>> getml.database.execute(stmt)
        >>>
        >>> stmt = data.database.read_csv(
        ...         fnames=["file1.csv", "file2.csv"],
        ...         name="MY_TABLE",
        ...         sep=';'
        ... )

    Args:
        name (str):
            Name of the table in which the data is to be inserted.

        fnames (List[str]):
            The list of CSV file names to be read.

        quotechar (str, optional):
            The character used to wrap strings. Default:`"`

        sep (str, optional):
            The separator used for separating fields. Default:`,`

        num_lines_read (int, optional):
            Number of lines read from each file.
            Set to 0 to read in the entire file.

        skip (int, optional):
            Number of lines to skip at the beginning of each
            file (Default: 0).

        colnames(List[str] or None, optional):
            The first line of a CSV file
            usually contains the column names. When this is not the case, you need to
            explicitly pass them.

        conn (:class:`~getml.database.Connection`, optional):
            The database connection to be used.
            If you don't explicitly pass a connection,
            the engine will use the default connection.

    """
    # -------------------------------------------

    conn = conn or Connection()

    # -------------------------------------------

    if not isinstance(fnames, list):
        fnames = [fnames]

    fnames_ = _retrieve_urls(fnames)

    # -------------------------------------------

    cmd: Dict[str, Any] = {}

    cmd["name_"] = name
    cmd["type_"] = "Database.read_csv"

    cmd["fnames_"] = fnames_
    cmd["quotechar_"] = quotechar
    cmd["sep_"] = sep
    cmd["skip_"] = skip
    cmd["num_lines_read_"] = num_lines_read
    cmd["conn_id_"] = conn.conn_id

    if colnames is not None:
        cmd["colnames_"] = colnames

    # -------------------------------------------

    comm.send(cmd)
