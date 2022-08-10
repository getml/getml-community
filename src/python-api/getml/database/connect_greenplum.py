# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Creates a new Greenplum database connection.
"""

from typing import Any, Dict, List, Optional

import getml.communication as comm
from getml import constants

from .connection import Connection


def connect_greenplum(
    dbname: str,
    user: str,
    password: str,
    host: str,
    hostaddr: str,
    port: int = 5432,
    time_formats: Optional[List[str]] = None,
    conn_id: str = "default",
):
    """Creates a new Greenplum database connection.

    But first, make sure your database is running and you can reach it
    from via your command line.

    Args:
        dbname (str):
            The name of the database to which you want to connect.

        user (str):
            User name with which to log into the Greenplum database.

        password (str):
            Password with which to log into the Greenplum database.

        host (str):
            Host of the Greenplum database.

        hostaddr (str):
            IP address of the Greenplum database.

        port(int, optional):
            Port of the Greenplum database.

            The default port used by Greenplum is 5432.

            If you do not know, which port to use, type the following into your
            Greenplum client:

            .. code-block:: sql

                SELECT setting FROM pg_settings WHERE name = 'port';

        time_formats (List[str], optional):
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

        conn_id (str, optional):
            The name to be used to reference the connection.
            If you do not pass anything, this will create a new default connection.

    Note:
        By selecting an existing table of your database in
        :func:`~getml.DataFrame.from_db` function, you can create
        a new :class:`~getml.DataFrame` containing all its data.
        Alternatively you can use the
        :meth:`~.getml.DataFrame.read_db` and
        :meth:`~.getml.DataFrame.read_query` methods to replace
        the content of the current :class:`~getml.DataFrame`
        instance or append further rows based on either a table or a
        specific query.

        You can also write your results back into the Greenplum
        database. By passing the name for the destination table to
        :meth:`getml.Pipeline.transform`, the features
        generated from your raw data will be written back. Passing
        them into :meth:`getml.Pipeline.predict`, instead,
        makes predictions of the target variables to new, unseen data
        and stores the result into the corresponding table.

    """

    time_formats = time_formats or constants.TIME_FORMATS

    cmd: Dict[str, Any] = {}

    cmd["name_"] = ""
    cmd["type_"] = "Database.new"
    cmd["db_"] = "greenplum"

    cmd["host_"] = host
    cmd["hostaddr_"] = hostaddr
    cmd["port_"] = port
    cmd["dbname_"] = dbname
    cmd["user_"] = user
    cmd["time_formats_"] = time_formats
    cmd["conn_id_"] = conn_id

    with comm.send_and_get_socket(cmd) as sock:
        # The password is sent separately, so it doesn't
        # end up in the logs.
        comm.send_string(sock, password)
        msg = comm.recv_string(sock)

    if msg != "Success!":
        comm.engine_exception_handler(msg)

    return Connection(conn_id=conn_id)
