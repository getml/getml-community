# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Parses the columns from a cmd"""

from typing import Any, Dict, TypeVar, Union

from .columns import (
    BooleanColumnView,
    FloatColumn,
    FloatColumnView,
    StringColumn,
    StringColumnView,
)
from .constants import (
    BOOLEAN_COLUMN_VIEW,
    FLOAT_COLUMN,
    FLOAT_COLUMN_VIEW,
    STRING_COLUMN,
    STRING_COLUMN_VIEW,
)

Coltype = TypeVar(
    "Coltype",
    FloatColumn,
    StringColumn,
    FloatColumnView,
    StringColumnView,
    BooleanColumnView,
)


def _make_column(cmd: Dict[str, Any], col: Coltype):
    col.cmd = cmd
    return col


def _parse(
    cmd: Dict[str, Any],
) -> Union[
    BooleanColumnView, FloatColumn, FloatColumnView, StringColumn, StringColumnView
]:
    typ = cmd["type_"]

    if typ == BOOLEAN_COLUMN_VIEW:
        return _make_column(cmd, BooleanColumnView("", None, None))

    if typ == FLOAT_COLUMN:
        return _make_column(cmd, FloatColumn())

    if typ == FLOAT_COLUMN_VIEW:
        return _make_column(cmd, FloatColumnView("", 0, 0))

    if typ == STRING_COLUMN:
        return _make_column(cmd, StringColumn())

    if typ == STRING_COLUMN_VIEW:
        return _make_column(cmd, StringColumnView("", "", ""))

    raise ValueError("Unknown column type: '" + typ + "'")
