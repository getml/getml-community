# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Transform column to a pyarrow.ChunkedArray
"""


from typing import Any, Dict

import pyarrow as pa  # type: ignore

import getml.communication as comm


def _to_arrow(self: Any, unique: bool = False) -> pa.ChunkedArray:
    """
    Transform column to numpy array
    """

    typename = type(self).__name__.replace("View", "")

    cmd: Dict[str, Any] = {}

    cmd["name_"] = ""
    cmd["type_"] = typename + (".unique" if unique else ".get")

    cmd["col_"] = self.cmd

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)

        if msg != "Success!":
            comm.engine_exception_handler(msg)

        with sock.makefile(mode="rb") as stream:
            with pa.ipc.open_stream(stream) as reader:
                return reader.read_all()["column"]
