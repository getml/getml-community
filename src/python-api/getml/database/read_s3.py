# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Reads a list of CSV files located in an S3 bucket.
"""

from typing import Any, Dict, List, Optional

import getml.communication as comm

from .connection import Connection


def read_s3(
    name: str,
    bucket: str,
    keys: List[str],
    region: str,
    sep: str = ",",
    num_lines_read: int = 0,
    skip: int = 0,
    colnames: Optional[List[str]] = None,
    conn: Optional[Connection] = None,
) -> None:
    """
    Reads a list of CSV files located in an S3 bucket.

    Args:
        name:
            Name of the table in which the data is to be inserted.

        bucket:
            The bucket from which to read the files.

        keys:
            The list of keys (files in the bucket) to be read.

        region:
            The region in which the bucket is located.

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
        *file2.csv* - in the bucket. You can
        import their data into the getML Engine using the following
        commands:
        ```python
        getml.engine.set_s3_access_key_id("YOUR-ACCESS-KEY-ID")

        getml.engine.set_s3_secret_access_key("YOUR-SECRET-ACCESS-KEY")

        stmt = data.database.sniff_s3(
                bucket="your-bucket-name",
                keys=["file1.csv", "file2.csv"],
                region="us-east-2",
                name="MY_TABLE",
                sep=';'
        )

        getml.database.execute(stmt)

        data.database.read_s3(
            bucket="your-bucket-name",
            keys=["file1.csv", "file2.csv"],
            region="us-east-2",
            name="MY_TABLE",
            sep=';'
        )
        ```
        You can also set the access credential as environment variables
        before you launch the getML Engine.

    """
    # -------------------------------------------

    conn = conn or Connection()

    # -------------------------------------------

    cmd: Dict[str, Any] = {}

    cmd["name_"] = name
    cmd["type_"] = "Database.read_s3"

    cmd["bucket_"] = bucket
    cmd["keys_"] = keys
    cmd["num_lines_read_"] = num_lines_read
    cmd["region_"] = region
    cmd["sep_"] = sep
    cmd["skip_"] = skip
    cmd["conn_id_"] = conn.conn_id

    if colnames is not None:
        cmd["colnames_"] = colnames

    # -------------------------------------------

    comm.send(cmd)
