# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Creates a new data frame by concatenating a list of existing ones.
"""

from typing import Any, Dict, List, Union

import getml.communication as comm

from .data_frame import DataFrame
from .helpers import _is_non_empty_typed_list
from .view import View

# --------------------------------------------------------------------


def concat(name: str, data_frames: List[Union[DataFrame, View]]):
    """
    Creates a new data frame by concatenating a list of existing ones.

    Args:
        name:
            Name of the new column.

        data_frames:
            The data frames to concatenate.
            Must be non-empty. However, it can contain only one data frame.
            Column names and roles must match.
            Columns will be appended by name, not order.

    Examples:
        ```python
        new_df = data.concat("NEW_DF_NAME", [df1, df2])
        ```
    """

    if not isinstance(name, str):
        raise TypeError("'name' must be a string.")

    if not _is_non_empty_typed_list(data_frames, (View, DataFrame)):
        raise TypeError(
            "'data_frames' must be a non-empty list of getml.data.Views "
            + "or getml.DataFrames."
        )

    cmd: Dict[str, Any] = {}

    cmd["type_"] = "DataFrame.concat"
    cmd["name_"] = name

    cmd["data_frames_"] = [df._getml_deserialize() for df in data_frames]

    comm.send(cmd)

    return DataFrame(name=name).refresh()


# --------------------------------------------------------------------
