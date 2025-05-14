# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
The unit of this column.
"""

from typing import Any, Dict

import getml.communication as comm

from .constants import VIEW_SIGNIFIER


@property  # type: ignore
def _unit(self) -> str:
    """
    The unit of this column.

    Units are used to determine which columns can be compared to each other
    by the feature learning algorithms.
    """

    cmd: Dict[str, Any] = {}

    cmd["name_"] = ""
    cmd["type_"] = (self.cmd["type_"] + ".get_unit").replace(VIEW_SIGNIFIER, "")

    cmd["col_"] = self.cmd

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.handle_engine_exception(msg)
        unit = comm.recv_string(sock)

    return unit
