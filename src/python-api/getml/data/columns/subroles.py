# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
The subroles of this column.
"""

from typing import Any, Dict, List

import getml.communication as comm

from .constants import VIEW_SIGNIFIER


@property  # type: ignore
def _subroles(self) -> List[str]:
    """
    The subroles of this column.

    Subroles are used for more fine-granular control
    what the column can be used for.
    """

    cmd: Dict[str, Any] = {}

    cmd["name_"] = ""
    cmd["type_"] = (self.cmd["type_"] + ".get_subroles").replace(VIEW_SIGNIFIER, "")

    cmd["col_"] = self.cmd

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.handle_engine_exception(msg)
        subroles = comm.recv_string_column(sock)

    return subroles.flatten().tolist()
