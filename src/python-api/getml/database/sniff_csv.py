# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Sniffs a list of CSV files.
"""

from typing import List, Optional, Union

from .connection import Connection
from .helpers import CSVCmdType, _read_csv, _retrieve_urls_for_engine


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
            the Engine will use the default connection.

    Returns:
        Appropriate `CREATE TABLE` statement.
    """

    return _read_csv(
        CSVCmdType.SNIFF,
        name,
        fnames,
        num_lines_sniffed,
        quotechar,
        sep,
        skip,
        colnames,
        conn,
    )
