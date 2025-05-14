# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Creates a new MariaDB database connection.
"""

from typing import Any, Dict, List, Optional

import getml.communication as comm
from getml import constants

from .connection import Connection


def connect_mariadb(
    dbname: str,
    user: str,
    password: str,
    host: str,
    port: int = 3306,
    unix_socket: str = "/var/run/mysqld/mysqld.sock",
    time_formats: Optional[List[str]] = None,
    conn_id: str = "default",
) -> Connection:
    """
    Creates a new MariaDB database connection.

    But first, make sure your database is running and you can reach it
    from via your command line.

    Args:
        dbname:
            The name of the database to which you want to connect.

        user:
            Username with which to log into the MariaDB database.

        password:
            Password with which to log into the MariaDB database.

        host:
            Host of the MariaDB database.

        port:
            Port of the MariaDB database.

            The default port for MariaDB is 3306.

            If you do not know which port to use, type

            ```sql
            SELECT @@port;
            ```
            into your MariaDB client.

        unix_socket:
            The UNIX socket used to connect to the MariaDB database.

            If you do not know which UNIX socket to use, type

            ```sql
            SELECT @@socket;
            ```
            into your MariaDB client.

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
        The connection object.

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

        You can also write your results back into the MariaDB
        database. By passing the name for the destination table to
        [`transform`][getml.Pipeline.transform], the features
        generated from your raw data will be written back. Passing
        them into [`predict`][getml.Pipeline.predict], instead,
        makes predictions
        of the target variables to new, unseen data and stores the result into
        the corresponding table.


    """

    time_formats = time_formats or constants.TIME_FORMATS

    cmd: Dict[str, Any] = {}

    cmd["name_"] = ""
    cmd["type_"] = "Database.new"
    cmd["db_"] = "mariadb"

    cmd["host_"] = host
    cmd["port_"] = port
    cmd["dbname_"] = dbname
    cmd["user_"] = user
    cmd["unix_socket_"] = unix_socket
    cmd["time_formats_"] = time_formats
    cmd["conn_id_"] = conn_id

    with comm.send_and_get_socket(cmd) as sock:
        # The password is sent separately, so it doesn't
        # end up in the logs.
        comm.send_string(sock, password)
        msg = comm.recv_string(sock)

    if msg != "Success!":
        comm.handle_engine_exception(msg)

    return Connection(conn_id=conn_id)
