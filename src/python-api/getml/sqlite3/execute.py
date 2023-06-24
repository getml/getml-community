# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Executes SQL scripts on SQLite3
"""

import os
import sqlite3
from typing import List


from .helpers import _log

# ----------------------------------------------------------------------------


def _retrieve_scripts(folder: str, file_type: str) -> List[str]:
    if folder[-1] != "/":
        folder = folder + "/"
    scripts = os.listdir(folder)
    scripts = [script for script in scripts if script[-len(file_type) :] == file_type]
    scripts = [folder + script for script in scripts]
    scripts.sort()
    return scripts


# ----------------------------------------------------------------------------


def execute(conn: sqlite3.Connection, fname: str):
    """
    Executes an SQL script or several SQL scripts on SQLite3.

    Args:
        conn (sqlite3.Connection):
            A sqlite3 connection created by :func:`~getml.sqlite3.connect`.

        fname (str):
            The names of the SQL script or a folder containing SQL scripts.
            If you decide to pass a folder, the SQL scripts must have the ending '.sql'.
    """
    # ------------------------------------------------------------

    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("'conn' must be an sqlite3.Connection object")

    if not isinstance(fname, str):
        raise TypeError("'fname' must be of type str")

    # ------------------------------------------------------------

    # Store temporary object in-memory.
    conn.execute("PRAGMA temp_store=2;")

    if os.path.isdir(fname):
        scripts = _retrieve_scripts(fname, ".sql")
        for script in scripts:
            execute(conn, script)
        return

    _log("Executing " + fname + "...")

    with open(fname, "rt", encoding="utf-8") as sqlfile:
        queries = sqlfile.read().split(";")

    for query in queries:
        conn.execute(query + ";")

    conn.commit()


# ----------------------------------------------------------------------------
