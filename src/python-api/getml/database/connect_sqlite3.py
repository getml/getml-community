# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Creates a new SQLite3 database connection.
"""

import os
from typing import Any, Dict, List, Optional

import getml.communication as comm
from getml import constants

from .connection import Connection


def connect_sqlite3(
    name: str = ":memory:",
    time_formats: Optional[List[str]] = None,
    conn_id: str = "default",
) -> Connection:
    """Creates a new SQLite3 database connection.

    SQLite3 is a popular in-memory database. It is faster than
    distributed databases, like PostgreSQL, but less stable under massive
    parallel access, consumes more memory and requires all contained
    data sets to be loaded into memory, which might fill up too much
    of your RAM, especially for large data sets.

    Args:
        name:
            Name of the sqlite3 file.  If the file does not exist, it
            will be created. Set to ":memory:" for a purely in-memory SQLite3
            database.

        time_formats:
            The list of formats tried when parsing time stamps.

            The formats are allowed to contain the following
            special characters:

            * %w - abbreviated weekday (Mon, Tue, ...)
            * %W - full weekday (Monday, Tuesday, ...)
            * %b - abbreviated month (Jan, Feb, ...)
            * %B - full month (January, February, ...)
            * %d - zero-padded day of month (01 .. 31)
            * %e - day of month (1 .. 31)
            * %f - space-padded day of month ( 1 .. 31)
            * %m - zero-padded month (01 .. 12)
            * %n - month (1 .. 12)
            * %o - space-padded month ( 1 .. 12)
            * %y - year without century (70)
            * %Y - year with century (1970)
            * %H - hour (00 .. 23)
            * %h - hour (00 .. 12)
            * %a - am/pm
            * %A - AM/PM
            * %M - minute (00 .. 59)
            * %S - second (00 .. 59)
            * %s - seconds and microseconds (equivalent to %S.%F)
            * %i - millisecond (000 .. 999)
            * %c - centisecond (0 .. 9)
            * %F - fractional seconds/microseconds (000000 - 999999)
            * %z - time zone differential in ISO 8601 format (Z or +NN.NN)
            * %Z - time zone differential in RFC format (GMT or +NNNN)
            * %% - percent sign

        conn_id:
            The name to be used to reference the connection.
            If you do not pass anything, this will create a new default connection.

    Returns:
        The new SQLite3 database connection.

    Note:
        By selecting an existing table of your database in
        [`from_db`][getml.DataFrame.from_db] function, you can create
        a new [`DataFrame`][getml.DataFrame] containing all its data.
        Alternatively you can use the
        [`read_db`][getml.DataFrame.read_db] and
        [`read_query`][getml.DataFrame.read_query] methods to replace
        the content of the current [`DataFrame`][getml.DataFrame]
        instance or append further rows based on either a table or a
        specific query.

        You can also write your results back into the SQLite3
        database. By passing the name for the destination table to
        [`transform`][getml.Pipeline.transform], the features
        generated from your raw data will be written back. Passing
        them into [`predict`][getml.Pipeline.predict], instead,
        makes predictions of the target variables to new, unseen data
        and stores the result into the corresponding table.

    """

    time_formats = time_formats or constants.TIME_FORMATS

    # We want to use the absolute path, unless it is a pure
    # in-memory version.
    name_ = name

    if name_ != ":memory:":
        name_ = os.path.abspath(name)

    cmd: Dict[str, Any] = {}

    cmd["name_"] = name_
    cmd["type_"] = "Database.new"

    cmd["db_"] = "sqlite3"
    cmd["time_formats_"] = time_formats
    cmd["conn_id_"] = conn_id

    with comm.send_and_get_socket(cmd) as sock:
        # The password is usually sent separately,
        # so it doesn't
        # end up in the logs. However, Sqlite3 does not
        # need a password, so we just send a dummy.
        comm.send_string(sock, "none")
        msg = comm.recv_string(sock)

    if msg != "Success!":
        comm.handle_engine_exception(msg, extra={"name": name_})

    return Connection(conn_id=conn_id)
