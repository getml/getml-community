# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Creates a new HANA database connection.
"""

from typing import Any, Dict, List, Optional

import getml.communication as comm
from getml import constants

from .connection import Connection


def connect_hana(
    user: str,
    password: str,
    host: str,
    port: int = 39017,
    default_schema: Optional[str] = "public",
    ping_interval: int = 0,
    time_formats: Optional[List[str]] = None,
    conn_id: str = "default",
) -> Connection:
    """
    Creates a new HANA database connection.

    enterprise-adm: Enterprise edition
        This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

        For licensing information and technical support, please [contact us][contact-page].

    Args:
        user:
            Username with which to log into the HANA database.

        password:
            Password with which to log into the HANA database.

        host:
            Host of the HANA database.

        port:
            Port of the database.

        default_schema:
            The schema within the database you want to connect
            use unless another schema is explicitly set.

        ping_interval:
            The interval at which you want to ping the database,
            in seconds. Set to 0 for no pings at all.

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

        conn_id (str, optional):
            The name to be used to reference the connection.
            If you do not pass anything, this will create a new default connection.

    Returns:
        The connection object.
    """

    time_formats = time_formats or constants.TIME_FORMATS

    cmd: Dict[str, Any] = {}

    cmd["name_"] = ""
    cmd["type_"] = "Database.new"
    cmd["db_"] = "sap_hana"

    cmd["host_"] = host
    cmd["port_"] = port
    cmd["default_schema_"] = default_schema
    cmd["user_"] = user
    cmd["ping_interval_"] = ping_interval
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
