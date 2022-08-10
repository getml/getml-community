# Copyright 2022 The SQLNet Company GmbH
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

from .constants import VIEW_SIGNIFIER


@property  # type: ignore
def _length_property(self) -> Union[np.int32, str]:
    """
    The length of the column (number of rows in the data frame).
    """

    cmd: Dict[str, Any] = {}

    cmd["type_"] = (self.cmd["type_"] + ".get_nrows").replace(VIEW_SIGNIFIER, "")
    cmd["name_"] = ""

    cmd["col_"] = self.cmd

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Found!":
            sock.close()
            comm.engine_exception_handler(msg)
        nrows = comm.recv_string(sock)

    try:
        return np.int32(nrows)
    except Exception:
        return nrows
