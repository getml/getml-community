# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Returns a table as a pandas.DataFrame.
"""

import sqlite3

import pandas as pd  # type: ignore

from .to_list import to_list


def to_pandas(conn, query):
    """
    Returns a table as a pandas.DataFrame.

    Args:
        conn:
            A sqlite3 connection created by :func:`~getml.sqlite3.connect`.

        query (str):
            The query used to get the table. You can also
            pass the name of the table, in which case the entire
            table will be imported.
    """
    # ------------------------------------------------------------

    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("'conn' must be an sqlite3.Connection object")

    if not isinstance(query, str):
        raise TypeError("'query' must be a str")

    # ------------------------------------------------------------

    colnames, data = to_list(conn, query)
    data_frame = pd.DataFrame(data)
    data_frame.columns = colnames
    return data_frame
