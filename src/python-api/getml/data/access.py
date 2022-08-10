# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Manages the access to various data sources.
"""

from typing import Any, Dict

import getml.communication as comm
from getml.engine import is_alive as _is_alive

# -----------------------------------------------------------------------------


def set_s3_access_key_id(value):
    """Sets the Access Key ID to S3.

    NOTE THAT S3 IS NOT SUPPORTED ON WINDOWS.

    In order to retrieve data from S3, you need to set the Access Key ID
    and the Secret Access Key. You can either set them as environment
    variables before you start the getML engine or you can set them from
    this module.

    Args:
        value (str):
            The value to which you want to set the Access Key ID.
    """

    if not isinstance(value, str):
        raise TypeError("'value' must be of type str")

    if not _is_alive():
        raise ConnectionRefusedError(
            """
        Cannot connect to getML engine.
        Make sure the engine is running on port '"""
            + str(comm.port)
            + """' and you are logged in.
        See `help(getml.engine)`."""
        )

    cmd: Dict[str, Any] = {}
    cmd["type_"] = "set_s3_access_key_id"
    cmd["name_"] = ""

    with comm.send_and_get_socket(cmd) as sock:
        comm.send_string(sock, value)
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.engine_exception_handler(msg)


# -----------------------------------------------------------------------------


def set_s3_secret_access_key(value):
    """Sets the Secret Access Key to S3.

    NOTE THAT S3 IS NOT SUPPORTED ON WINDOWS.

    In order to retrieve data from S3, you need to set the Access Key ID
    and the Secret Access Key. You can either set them as environment
    variables before you start the getML engine or you can set them from
    this module.

    Args:
        value (str):
            The value to which you want to set the Secret Access Key.
    """

    if not isinstance(value, str):
        raise TypeError("'value' must be of type str")

    if not _is_alive():
        raise ConnectionRefusedError(
            """
        Cannot connect to getML engine.
        Make sure the engine is running on port '"""
            + str(comm.port)
            + """' and you are logged in.
        See `help(getml.engine)`."""
        )

    cmd: Dict[str, Any] = {}
    cmd["type_"] = "set_s3_secret_access_key"
    cmd["name_"] = ""

    with comm.send_and_get_socket(cmd) as sock:
        comm.send_string(sock, value)
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.engine_exception_handler(msg)


# --------------------------------------------------------------------------

__all__ = ("set_s3_access_key_id", "set_s3_secret_access_key")
