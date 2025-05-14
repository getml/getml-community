# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Lazily evaluated aggregation over a column.
"""

from typing import Any, Dict

import getml.communication as comm


class Aggregation:
    """
    Lazily evaluated aggregation over a column.

    ??? example
        ```python
        my_data_frame["my_column"].avg()
        3.0
        ```
    """

    def __init__(self, alias, col, agg_type):
        self.cmd: Dict[str, Any] = {}
        self.cmd["as_"] = alias
        self.cmd["col_"] = col.cmd
        self.cmd["type_"] = agg_type

    # -----------------------------------------------------------------------------

    def __repr__(self):
        return str(self)

    # -----------------------------------------------------------------------------

    def __str__(self):
        val = self.get()
        return self.cmd["type_"].upper() + " aggregation, value: " + str(val) + "."

    # --------------------------------------------------------------------------

    def get(self):
        """
        Receives the value of the aggregation over the column.
        """

        cmd: Dict[str, Any] = {}

        cmd["name_"] = ""
        cmd["type_"] = "FloatColumn.aggregate"

        cmd["aggregation_"] = self.cmd

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)
            if msg != "Success!":
                comm.handle_engine_exception(msg)
            mat = comm.recv_float_matrix(sock)

        return mat.ravel()[0]
