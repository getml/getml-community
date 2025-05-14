# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Generates an appropriate column from a value.
"""

import numbers
from typing import Union

import numpy as np

from .columns import BooleanColumnView, FloatColumnView, StringColumnView, _value_to_cmd

# -----------------------------------------------------------------------------

ReturnType = Union[BooleanColumnView, StringColumnView, FloatColumnView]
"""
Possible return types of the function `from_value`.
"""

# -----------------------------------------------------------------------------


def from_value(val: Union[bool, str, int, float, np.datetime64]) -> ReturnType:
    """
    Creates an infinite column that contains the same
    value in all of its elements.

    Args:
        val:
            The value you want to insert into your column.

    Returns:
        The column view containing the value.
    """
    cmd = _value_to_cmd(val)

    if isinstance(val, bool):
        col: ReturnType = BooleanColumnView(
            operator="const",
            operand1=None,
            operand2=None,
        )
        col.cmd = cmd
        return col

    if isinstance(val, str):
        col = StringColumnView(
            operator="const",
            operand1=val,
            operand2=None,
        )
        col.cmd = cmd
        return col

    if isinstance(val, (int, float, numbers.Number)):
        col = FloatColumnView(
            operator="const",
            operand1=val,
            operand2=None,
        )
        col.cmd = cmd
        return col

    if isinstance(val, np.datetime64):
        col = FloatColumnView(
            operator="const",
            operand1=np.datetime64(val, "s").astype(float),
            operand2=None,
        )
        col.cmd = cmd
        return col

    raise TypeError("val must be bool, str or a number.")
