# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
The length of the column (number of rows in the data frame).
"""

from typing import Any, Dict, Union

import numpy as np

import getml.communication as comm
from getml.data.columns.constants import VIEW_SIGNIFIER
from getml.utilities.formatting.column_formatter import _ColumnFormatter


@property  # type: ignore
def _length_property(col) -> Union[int, str]:
    """
    The length of the column (number of rows in the data frame).
    """

    cmd: Dict[str, Any] = {}

    cmd["type_"] = (col.cmd["type_"] + ".get_nrows").replace(VIEW_SIGNIFIER, "")
    cmd["name_"] = ""

    cmd["col_"] = col.cmd

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Found!":
            comm.handle_engine_exception(msg)
        nrows = comm.recv_string(sock)

    nrows_to_display = len(col[: _ColumnFormatter.max_rows + 1].to_numpy())
    if nrows_to_display <= _ColumnFormatter.max_rows:
        return nrows_to_display

    try:
        return int(nrows)
    except:
        return nrows
