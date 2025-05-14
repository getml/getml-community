# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Reads a CSV file into the database.
"""

from typing import List, Optional, Union

from .connection import Connection
from .helpers import (
    CSVCmdType,
    _read_csv,
)


def read_csv(
    name: str,
    fnames: Union[str, List[str]],
    quotechar: str = '"',
    sep: str = ",",
    num_lines_read: int = 0,
    skip: int = 0,
    colnames: Optional[List[str]] = None,
    conn: Optional[Connection] = None,
) -> None:
    """
    Reads a CSV file into the database.

    Args:
        name:
            Name of the table in which the data is to be inserted.

        fnames:
            The list of CSV file names to be read.

        quotechar:
            The character used to wrap strings. Default:`"`

        sep:
            The separator used for separating fields. Default:`,`

        num_lines_read:
            Number of lines read from each file.
            Set to 0 to read in the entire file.

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

    ??? example
        Let's assume you have two CSV files - *file1.csv* and
        *file2.csv* . You can import their data into the database
        using the following commands:

        ```python
        stmt = data.database.sniff_csv(
                fnames=["file1.csv", "file2.csv"],
                name="MY_TABLE",
                sep=';'
        )

        getml.database.execute(stmt)

        data.database.read_csv(
            fnames=["file1.csv", "file2.csv"],
            name="MY_TABLE",
            sep=';'
        )
        ```

    """

    return _read_csv(
        CSVCmdType.READ,
        name,
        fnames,
        num_lines_read,
        quotechar,
        sep,
        skip,
        colnames,
        conn,
    )
