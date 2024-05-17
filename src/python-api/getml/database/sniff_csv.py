# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Sniffs a list of CSV files.
"""

from typing import Any, Dict, List, Optional, Union

import getml.communication as comm

from .connection import Connection
from .helpers import _retrieve_urls


def sniff_csv(
    name: str,
    fnames: Union[str, List[str]],
    num_lines_sniffed: int = 1000,
    quotechar: str = '"',
    sep: str = ",",
    skip: int = 0,
    colnames: Optional[List[str]] = None,
    conn: Optional[Connection] = None,
) -> str:
    """
    Sniffs a list of CSV files.

    Args:
        name:
            Name of the table in which the data is to be inserted.

        fnames:
            The list of CSV file names to be read.

        num_lines_sniffed:
            Number of lines analyzed by the sniffer.

        quotechar:
            The character used to wrap strings. Default:`"`

        sep:
            The separator used for separating fields. Default:`,`

        skip:
            Number of lines to skip at the beginning of each
            file (Default: 0).

        colnames:
            The first line of a CSV file
            usually contains the column names. When this is not the case, you need to
            explicitly pass them.

        conn:
            The database connection to be used.
            If you don't explicitly pass a connection,
            the engine will use the default connection.

    Returns:
        Appropriate `CREATE TABLE` statement.
    """

    conn = conn or Connection()

    if not isinstance(fnames, list):
        fnames = [fnames]

    fnames_ = _retrieve_urls(fnames)

    cmd: Dict[str, Any] = {}

    cmd["name_"] = name
    cmd["type_"] = "Database.sniff_csv"

    cmd["fnames_"] = fnames_
    cmd["num_lines_sniffed_"] = num_lines_sniffed
    cmd["quotechar_"] = quotechar
    cmd["sep_"] = sep
    cmd["skip_"] = skip
    cmd["conn_id_"] = conn.conn_id

    if colnames is not None:
        cmd["colnames_"] = colnames

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            sock.close()
            comm.engine_exception_handler(msg)
        return comm.recv_string(sock)
