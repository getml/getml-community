# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Creates a new BigQuery database connection.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import getml.communication as comm
from getml import constants

from .connection import Connection


def connect_bigquery(
    database_id: str,
    project_id: str,
    google_application_credentials: Union[str, Path],
    time_formats: Optional[List[str]] = None,
    conn_id: str = "default",
) -> Connection:
    """
    Creates a new BigQuery database connection.

    enterprise-adm: Enterprise edition
        This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

        For licensing information and technical support, please [contact us][contact-page].

    Args:
        database_id:
            The ID of the database to connect to.

        project_id:
            The ID of the project to connect to.

        google_application_credentials:
            The path of the Google application credentials.
            (Must be located on the machine hosting the getML Engine).

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
    """

    time_formats = time_formats or constants.TIME_FORMATS

    cmd: Dict[str, Any] = {}

    cmd["database_id_"] = database_id
    cmd["project_id_"] = project_id
    cmd["google_application_credentials_"] = os.path.abspath(
        str(google_application_credentials)
    )
    cmd["name_"] = ""
    cmd["type_"] = "Database.new"
    cmd["db_"] = "bigquery"

    cmd["time_formats_"] = time_formats
    cmd["conn_id_"] = conn_id

    with comm.send_and_get_socket(cmd) as sock:
        # The API expects a password, but in this case there is none
        comm.send_string(sock, "")
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.handle_engine_exception(msg)

    return Connection(conn_id=conn_id)
