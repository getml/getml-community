# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Returns the last time a data frame has been changed.
"""

from typing import Any, Dict

import getml.communication as comm


def _last_change(df_name) -> str:

    cmd: Dict[str, Any] = {}
    cmd["type_"] = "DataFrame.last_change"
    cmd["name_"] = df_name

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.engine_exception_handler(msg)
        result = comm.recv_string(sock)

    return result
