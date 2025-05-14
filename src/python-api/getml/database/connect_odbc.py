# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Creates a new ODBC database connection.
"""

from typing import Any, Dict, List, Optional

import getml.communication as comm
from getml import constants

from .connection import Connection


def connect_odbc(
    server_name: str,
    user: str = "",
    password: str = "",
    escape_chars: str = '"',
    double_precision: str = "DOUBLE PRECISION",
    integer: str = "INTEGER",
    text: str = "TEXT",
    time_formats: Optional[List[str]] = None,
    conn_id: str = "default",
) -> Connection:
    """
    Creates a new ODBC database connection.

    ODBC is standardized format that can be used to connect to almost any
    database.

    Before you use the ODBC connector, make sure the database is up and
    running and that the appropriate ODBC drivers are installed.

    enterprise-adm: Enterprise edition
        This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

        For licensing information and technical support, please [contact us][contact-page].

    Args:
        server_name:
            The server name, as referenced in your .obdc.ini file.

        user:
            Username with which to log into the database.
            You do not need to pass this, if it is already contained in your
            .odbc.ini.

        password:
            Password with which to log into the database.
            You do not need to pass this, if it is already contained in your
            .odbc.ini.

        escape_chars:
            ODBC drivers are supposed to support
            escaping table and column names using '"' characters irrespective of the
            syntax in the target database. Unfortunately, not all ODBC drivers
            follow this standard. This is why some
            tuning might be necessary.

            The escape_chars value behaves as follows:

            * If you pass an empty string, schema, table and column names will not
              be escaped at all. This is not a problem unless some table
              or column names are identical to SQL keywords.

            * If you pass a single character, schema, table and column names will
              be enveloped in that character: "TABLE_NAME"."COLUMN_NAME" (standard SQL)
              or `TABLE_NAME`.`COLUMN_NAME` (MySQL/MariaDB style).

            * If you pass two characters, table, column and schema names will be
              enveloped between these to characters. For instance, if you pass "[]",
              the produced queries look as follows:
              [TABLE_NAME].[COLUMN_NAME] (MS SQL Server style).

            * If you pass more than two characters, the Engine will throw an exception.

        double_precision:
            The keyword used for double precision columns.

        integer:
            The keyword used for integer columns.

        text:
            The keyword used for text columns.

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

        conn_id:
            The name to be used to reference the connection.
            If you do not pass anything, this will create a new default connection.

    Returns:
        The connection object.

    """

    time_formats = time_formats or constants.TIME_FORMATS

    cmd: Dict[str, Any] = {}

    cmd["name_"] = ""
    cmd["type_"] = "Database.new"
    cmd["db_"] = "odbc"

    cmd["server_name_"] = server_name
    cmd["user_"] = user
    cmd["escape_chars_"] = escape_chars
    cmd["double_precision_"] = double_precision
    cmd["integer_"] = integer
    cmd["text_"] = text
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
