# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Collection of helper functions that are not intended to be used by
the end-user.
"""

import json
from typing import Any, Dict

import getml.communication as comm


def _get_column_content(col, coltype, start, length):
    """
    Returns the contents of a data frame in a format that is
    compatible with jquery.data.tables.

    Args:
        col (dict): The cmd describing the dict.

        coltype (str): The type of the column
            (FloatColumn, StringColumn or BooleanColumn).

        start (int): The number of the first line to retrieve.

        length (int): The number of lines to retrieve.
    """

    if not isinstance(start, int):
        raise TypeError("'start' must be an int.")

    if not isinstance(length, int):
        raise TypeError("'length' must be an int.")

    cmd: Dict[str, Any] = {}

    cmd["type_"] = coltype + ".get_content"
    cmd["name_"] = ""

    cmd["col_"] = col
    cmd["draw_"] = 1

    cmd["start_"] = start
    cmd["length_"] = length

    with comm.send_and_get_socket(cmd) as sock:
        json_str = comm.recv_string(sock)
        if json_str[0] != "{":
            comm.handle_engine_exception(json_str)

    return json.loads(json_str)


# --------------------------------------------------------------------


def _get_data_frame_content(name, start, length):
    """
    Returns the contents of a data frame in a format that is
    compatible with jquery.data.tables.

    Args:
        name (str): The name of the data frame.

        start (int): The number of the first line to retrieve.

        length (int): The number of lines to retrieve.
    """

    if not isinstance(name, str):
        raise TypeError("'name' must be a str.")

    if not isinstance(start, int):
        raise TypeError("'start' must be an int.")

    if not isinstance(start, int):
        raise TypeError("'length' must be an int.")

    cmd: Dict[str, Any] = {}

    cmd["type_"] = "DataFrame.get_content"
    cmd["name_"] = name

    cmd["start_"] = start
    cmd["length_"] = length

    cmd["draw_"] = 1

    with comm.send_and_get_socket(cmd) as sock:
        json_str = comm.recv_string(sock)

    if json_str[0] != "{":
        comm.handle_engine_exception(json_str)

    return json.loads(json_str)


# --------------------------------------------------------------------


def _get_view_content(start, length, cols):
    cmd: Dict[str, Any] = {}

    cmd["type_"] = "View.get_content"
    cmd["name_"] = ""

    cmd["start_"] = start
    cmd["length_"] = length
    cmd["cols_"] = cols

    cmd["draw_"] = 1

    with comm.send_and_get_socket(cmd) as sock:
        json_str = comm.recv_string(sock)
        if json_str[0] != "{":
            comm.handle_engine_exception(json_str)

    return json.loads(json_str)
