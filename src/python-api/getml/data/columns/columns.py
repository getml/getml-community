# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Contains the actual columns."""

from __future__ import annotations

import numbers
import warnings
from abc import ABC
from collections import deque
from inspect import cleandoc
from typing import Any, Dict, Iterable, List, Optional, Union, get_args

import numpy as np
import pandas as pd
import pyarrow as pa

from getml.constants import TIME_FORMATS
from getml.data import roles
from getml.data._io.arrow import postprocess_arrow_schema
from getml.data.roles.container import Roles

from .aggregation import Aggregation
from .collect_footer_data import _collect_footer_data
from .column import _Column
from .constants import (
    BOOLEAN_COLUMN_VIEW,
    FLOAT_COLUMN,
    FLOAT_COLUMN_VIEW,
    INFINITE,
    STRING_COLUMN,
    STRING_COLUMN_VIEW,
)
from .format import _format
from .get_scalar import _get_scalar
from .last_change_from_col import _last_change_from_col
from .length import _length
from .length_property import _length_property
from .make_iter import _make_iter
from .repr import _repr
from .repr_html import _repr_html
from .subroles import _subroles
from .to_numpy import _to_numpy
from .unique import _unique
from .unit import _unit

# -----------------------------------------------------------------------------

TYPE_MISSMATCH_ERROR_MSG_TEMPLATE = cleandoc(
    """
    Can only compare {col_type} with: {operand_types}.

    You can explicitly cast columns via `.as_num()` or `.as_str()`.
    """
)

# -----------------------------------------------------------------------------

TIME_STAMP_PARSING_LIKELY_FAILED_WARNING_TEMPLATE = cleandoc(
    """
    After parsing time stamps, column '{column}' likely contains only nan values.

    Check the supplied 'time_formats' and the data.
    """
)

# -----------------------------------------------------------------------------


class _View(ABC):
    cmd: Dict[str, Any]


# -----------------------------------------------------------------------------


def _infer_column_name_recursive(cmd: Dict[str, Any]) -> str:
    """
    Infer the name from a ...Column(View) if the column is an (anonymous) view,
    recursively consult the view's bases (operands) in breadth-first manner
    until a name is found. If no name is found, return the view's type and
    operator.

    This function is used to construct informative error messages.
    """
    queue = deque([cmd])

    while queue:
        current = queue.popleft()

        if "name_" in current:
            return current["name_"]

        for key, value in current.items():
            if isinstance(value, dict):
                queue.append(value)

    type = cmd.get("type_")
    operator = cmd.get("operator_")
    return f"{type}.{operator}"


# -----------------------------------------------------------------------------


def arange(
    start: Union[numbers.Real, float] = 0.0,
    stop: Optional[Union[numbers.Real, float]] = None,
    step: Union[numbers.Real, float] = 1.0,
):
    """
    Returns evenly spaced variables, within a given interval.

    Args:
        start:
            The beginning of the interval. Defaults to 0.

        stop:
            The end of the interval.

        step:
            The step taken. Defaults to 1.
    """
    if stop is None:
        stop = start
        start = 0.0

    if step is None:
        step = 1.0

    if not isinstance(start, numbers.Real):
        raise TypeError("'start' must be a real number")

    if not isinstance(stop, numbers.Real):
        raise TypeError("'stop' must be a real number")

    if not isinstance(step, numbers.Real):
        raise TypeError("'step' must be a real number")

    col = FloatColumnView(
        operator="arange",
        operand1=None,
        operand2=None,
    )

    col.cmd["start_"] = float(start)
    col.cmd["stop_"] = float(stop)
    col.cmd["step_"] = float(step)

    return col


# -----------------------------------------------------------------------------


def rowid() -> FloatColumnView:
    """
    Get the row numbers of the table.

    Returns:
            (numerical) column containing the row id, starting with 0
    """
    return FloatColumnView(operator="rowid", operand1=None, operand2=None)


# -----------------------------------------------------------------------------


def _make_slicing_operand(column, slc):
    step = slc.step or 1
    start = slc.start or 0
    start = start if start >= 0 else len(column) + start
    if not slc.stop:
        if column.length == INFINITE:
            return (rowid() > start) & ((rowid() - start) % step == 0)  # type: ignore
        return arange(start, column.length, step)
    stop = slc.stop if slc.stop >= 0 else len(column) + slc.stop
    return arange(start, stop, step)


# -----------------------------------------------------------------------------


def _value_to_cmd(val: OperandType):
    cmd: Dict[str, Any] = {}

    cmd["operator_"] = "const"
    cmd["value_"] = val

    if isinstance(val, bool):
        cmd["type_"] = BOOLEAN_COLUMN_VIEW
        return cmd

    if isinstance(val, str):
        cmd["type_"] = STRING_COLUMN_VIEW
        return cmd

    if isinstance(val, (numbers.Real, float, int)):
        cmd["type_"] = FLOAT_COLUMN_VIEW
        cmd["value_"] = float(val)  # type: ignore
        return cmd

    if isinstance(val, np.datetime64):
        return _value_to_cmd(np.datetime64(val).astype("datetime64[s]").astype(float))

    assert False, "Calling _value_to_cmd on an unknown type"


# -----------------------------------------------------------------------------


class BooleanColumnView(_View):
    """
    Handle for a lazily evaluated boolean column view.

    Column views do not actually exist - they will be lazily
    evaluated when necessary.

    They can be used to take subselection of the data frame
    or to update other columns.

    ??? example
        ```python
        import numpy as np

        import getml.data as data
        import getml.engine as engine
        import getml.data.roles as roles

        # ----------------

        engine.set_project("examples")

        # ----------------
        # Create a data frame from a JSON string

        json_str = \"\"\"{
            "names": ["patrick", "alex", "phil", "ulrike"],
            "column_01": [2.4, 3.0, 1.2, 1.4],
            "join_key": ["0", "1", "2", "3"],
            "time_stamp": ["2019-01-01", "2019-01-02", "2019-01-03", "2019-01-04"]
        }\"\"\"

        my_df = data.DataFrame(
            "MY DF",
            roles={
                "unused_string": ["names", "join_key", "time_stamp"],
                "unused_float": ["column_01"]}
        ).read_json(
            json_str
        )

        # ----------------

        names = my_df["names"]

        # This is a virtual boolean column.
        a_or_p_in_names = names.contains("p") | names.contains("a")

        # Creates a view containing
        # only those entries, where "names" contains a or p.
        my_view = my_df[a_or_p_in_names]

        # ----------------

        # Returns a new column, where all names
        # containing "rick" are replaced by "Patrick".
        # Again, columns are immutable - this returns an updated
        # version, but leaves the original column unchanged.
        new_names = names.update(names.contains("rick"), "Patrick")

        my_df["new_names"] = new_names

        # ----------------

        # Boolean columns can also be used to
        # create binary target variables.
        target = (names == "phil")

        my_df["target"] = target
        my_df.set_role(target, roles.target)

        # By the way, instead of using the
        # __setitem__ operator and .set_role(...)
        # you can just use .add(...).
        my_df.add(target, "target", roles.target)
        ```
    """

    def __init__(
        self,
        operator: str,
        operand1: Optional[OperandType],
        operand2: Optional[OperandType],
    ):
        self.cmd: Dict[str, Any] = {}

        self.cmd["type_"] = BOOLEAN_COLUMN_VIEW

        self.cmd["operator_"] = operator

        if operand1 is not None:
            self.cmd["operand1_"] = self._parse_operand(operand1)

        if operand2 is not None:
            self.cmd["operand2_"] = self._parse_operand(operand2)

    # -----------------------------------------------------------------------------

    def __and__(self, other):
        return BooleanColumnView(
            operator="and",
            operand1=self,
            operand2=other,
        )

    # -----------------------------------------------------------------------------

    def __eq__(self, other):
        return BooleanColumnView(
            operator="bool_equal_to",
            operand1=self,
            operand2=other,
        )

    # -----------------------------------------------------------------------------

    def __invert__(self):
        return self.is_false()

    # -----------------------------------------------------------------------------

    def __or__(self, other):
        return BooleanColumnView(
            operator="or",
            operand1=self,
            operand2=other,
        )

    # -----------------------------------------------------------------------------

    def __ne__(self, other):
        return BooleanColumnView(
            operator="bool_not_equal_to",
            operand1=self,
            operand2=other,
        )

    # -----------------------------------------------------------------------------

    def __xor__(self, other):
        return BooleanColumnView(
            operator="xor",
            operand1=self,
            operand2=other,
        )

    # -----------------------------------------------------------------------------

    def _parse_operand(
        self,
        operand: OperandType,
    ):
        if isinstance(operand, (bool, str, numbers.Real, np.datetime64)):
            return _value_to_cmd(operand)

        if not hasattr(operand, "cmd"):
            raise TypeError(
                """Operand for a BooleanColumnView must be a
                boolean, string, a Real, a numpy.datetime64
                or a getml.data.Column!"""
            )

        if self.cmd["operator_"] in ["and", "or", "not", "xor"]:
            if operand.cmd["type_"] != BOOLEAN_COLUMN_VIEW:
                raise TypeError("This operator can only be applied to a BooleanColumn!")

        return operand.cmd

    # -----------------------------------------------------------------------------

    def is_false(self):
        """Whether an entry is False - effectively inverts the Boolean column."""
        return BooleanColumnView(
            operator="not",
            operand1=self,
            operand2=None,
        )

    # -----------------------------------------------------------------------------

    def as_num(self):
        """Transforms the boolean column into a numerical column"""
        return FloatColumnView(
            operator="boolean_as_num",
            operand1=self,
            operand2=None,
        )


# -----------------------------------------------------------------------------


class StringColumn(_Column):
    """Handle for categorical data that is kept in the getML Engine

    Attributes:
        name:
            Name of the categorical column.

        role:
            Role that the column plays.

        df_name:
            ``name`` instance variable of the
            [`DataFrame`][getml.DataFrame] containing this column.

    ??? example
        ```python
        import numpy as np

        import getml.data as data
        import getml.engine as engine
        import getml.data.roles as roles

        # ----------------

        engine.set_project("examples")

        # ----------------
        # Create a data frame from a JSON string

        json_str = \"\"\"{
            "names": ["patrick", "alex", "phil", "ulrike"],
            "column_01": [2.4, 3.0, 1.2, 1.4],
            "join_key": ["0", "1", "2", "3"],
            "time_stamp": ["2019-01-01", "2019-01-02", "2019-01-03", "2019-01-04"]
        }\"\"\"

        my_df = data.DataFrame(
            "MY DF",
            roles={
                "unused_string": ["names", "join_key", "time_stamp"],
                "unused_float": ["column_01"]}
        ).read_json(
            json_str
        )

        # ----------------

        col1 = my_df["names"]

        # ----------------

        col2 = col1.substr(4, 3)

        my_df.add(col2, "short_names", roles.categorical)

        # ----------------
        # If you do not explicitly set a role,
        # the assigned role will either be
        # roles.unused_string.

        col3 = "user-" + col1 + "-" + col2

        my_df["new_names"] = col3
        my_df.set_role("new_names", roles.categorical)
        ```
    """

    _num_columns = 0

    def __init__(self, name: str = "", role: str = "categorical", df_name: str = ""):
        super().__init__()

        StringColumn._num_columns += 1
        if name == "":
            name = STRING_COLUMN + " " + str(StringColumn._num_columns)

        self.cmd: Dict[str, Any] = {}

        self.cmd["operator_"] = STRING_COLUMN
        self.cmd["df_name_"] = df_name
        self.cmd["name_"] = name
        self.cmd["role_"] = role
        self.cmd["type_"] = STRING_COLUMN


# -----------------------------------------------------------------------------


class StringColumnView(_View):
    """
    Lazily evaluated view on a [`StringColumn`][getml.data.columns.StringColumn].

    Columns views do not actually exist - they will be lazily
    evaluated when necessary.

    ??? example
        ```python
        import numpy as np

        import getml.data as data
        import getml.engine as engine
        import getml.data.roles as roles

        # ----------------

        engine.set_project("examples")

        # ----------------
        # Create a data frame from a JSON string

        json_str = \"\"\"{
            "names": ["patrick", "alex", "phil", "ulrike"],
            "column_01": [2.4, 3.0, 1.2, 1.4],
            "join_key": ["0", "1", "2", "3"],
            "time_stamp": ["2019-01-01", "2019-01-02", "2019-01-03", "2019-01-04"]
        }\"\"\"

        my_df = data.DataFrame(
            "MY DF",
            roles={
                "unused_string": ["names", "join_key", "time_stamp"],
                "unused_float": ["column_01"]}
        ).read_json(
            json_str
        )

        # ----------------

        col1 = my_df["names"]

        # ----------------

        # col2 is a virtual column.
        # The substring operation is not
        # executed yet.
        col2 = col1.substr(4, 3)

        # This is where the Engine executes
        # the substring operation.
        my_df.add(col2, "short_names", roles.categorical)

        # ----------------
        # If you do not explicitly set a role,
        # the assigned role will either be
        # roles.unused_string.

        # col3 is a virtual column.
        # The operation is not
        # executed yet.
        col3 = "user-" + col1 + "-" + col2

        # This is where the operation is
        # is executed.
        my_df["new_names"] = col3
        my_df.set_role("new_names", roles.categorical)
        ```
    """

    def __init__(
        self,
        operator: str,
        operand1: Optional[Union[str, _Column, _View]],
        operand2: Optional[Union[str, _Column, _View]],
    ):
        self.cmd: Dict[str, Any] = {}

        self.cmd["type_"] = STRING_COLUMN_VIEW
        self.cmd["operator_"] = operator
        if operand1 is not None:
            self.cmd["operand1_"] = self._parse_operand(operand1)
        if operand2 is not None:
            self.cmd["operand2_"] = self._parse_operand(operand2)

    # -----------------------------------------------------------------------------

    def _parse_operand(self, operand: Union[str, _Column, _View]):
        if isinstance(operand, str):
            return _value_to_cmd(operand)

        if not hasattr(operand, "cmd"):
            raise TypeError(
                """Operand for a StringColumnView must
                   be a string or a column!"""
            )

        oper = self.cmd["operator_"]

        optype = operand.cmd["type_"]

        if oper == "as_str":
            wrong_coltype = optype not in [
                FLOAT_COLUMN,
                FLOAT_COLUMN_VIEW,
                BOOLEAN_COLUMN_VIEW,
            ]
            if wrong_coltype:
                raise TypeError(
                    "This operator can only be applied to a "
                    + "FloatColumn or a BooleanColumn!"
                )

        elif oper == "str_subselection":
            wrong_coltype = optype not in [
                STRING_COLUMN,
                STRING_COLUMN_VIEW,
                FLOAT_COLUMN,
                FLOAT_COLUMN_VIEW,
                BOOLEAN_COLUMN_VIEW,
            ]
            if wrong_coltype:
                raise TypeError(
                    "Columns or Views can only be subset by "
                    + "StringColumn) or a BooleanColumn!"
                )

        else:
            wrong_coltype = optype not in [STRING_COLUMN, STRING_COLUMN_VIEW]
            if wrong_coltype:
                raise TypeError("This operator can only be applied to a StringColumn!")

        return operand.cmd


# -----------------------------------------------------------------------------


class FloatColumn(_Column):
    """Handle for numerical data in the Engine.

    This is a handler for all numerical data in the getML Engine,
    including time stamps.

    Attributes:
        name:
            Name of the categorical column.

        role:
            Role that the column plays.

        df_name:
            ``name`` instance variable of the
            [`DataFrame`][getml.DataFrame]  containing this column.

    ??? example
        ```python
        import numpy as np

        import getml.data as data
        import getml.engine as engine
        import getml.data.roles as roles

        # ----------------

        engine.set_project("examples")

        # ----------------
        # Create a data frame from a JSON string

        json_str = \"\"\"{
            "names": ["patrick", "alex", "phil", "ulrike"],
            "column_01": [2.4, 3.0, 1.2, 1.4],
            "join_key": ["0", "1", "2", "3"],
            "time_stamp": ["2019-01-01", "2019-01-02", "2019-01-03", "2019-01-04"]
        }\"\"\"

        my_df = data.DataFrame(
            "MY DF",
            roles={
                "unused_string": ["names", "join_key", "time_stamp"],
                "unused_float": ["column_01"]}
        ).read_json(
            json_str
        )

        # ----------------

        col1 = my_df["column_01"]

        # ----------------

        col2 = 2.0 - col1

        my_df.add(col2, "name", roles.numerical)

        # ----------------
        # If you do not explicitly set a role,
        # the assigned role will either be
        # roles.unused_float.

        col3 = (col1 + 2.0*col2) / 3.0

        my_df["column_03"] = col3
        my_df.set_role("column_03", roles.numerical)
        ```
    """

    _num_columns = 0

    def __init__(self, name: str = "", role: str = "numerical", df_name: str = ""):
        super().__init__()

        FloatColumn._num_columns += 1
        if name == "":
            name = FLOAT_COLUMN + " " + str(FloatColumn._num_columns)

        self.cmd: Dict[str, Any] = {}

        self.cmd["operator_"] = FLOAT_COLUMN

        self.cmd["df_name_"] = df_name

        self.cmd["name_"] = name

        self.cmd["role_"] = role

        self.cmd["type_"] = FLOAT_COLUMN


# -----------------------------------------------------------------------------


class FloatColumnView(_View):
    """
    Lazily evaluated view on a [`FloatColumn`][getml.data.columns.FloatColumn].

    Column views do not actually exist - they will be lazily
    evaluated when necessary.
    """

    def __init__(
        self,
        operator: str,
        operand1: Optional[FloatOperandType],
        operand2: Optional[FloatOperandType],
    ):
        self.cmd: Dict[str, Any] = {}

        self.cmd["type_"] = FLOAT_COLUMN_VIEW

        self.cmd["operator_"] = operator

        if operand1 is not None:
            self.cmd["operand1_"] = self._parse_operand(operand1)

        if operand2 is not None:
            self.cmd["operand2_"] = self._parse_operand(operand2)

    # -----------------------------------------------------------------------------

    def _parse_operand(self, operand: FloatOperandType):
        if isinstance(operand, (numbers.Real, np.datetime64)):
            return _value_to_cmd(operand)

        if not hasattr(operand, "cmd"):
            raise TypeError(
                """Operand for a FloatColumnView must
                   be a number or a column!"""
            )

        special_ops = ["as_num", "as_ts", "boolean_as_num", "num_subselection"]
        oper = self.cmd["operator_"]
        optype = operand.cmd["type_"]

        if oper not in special_ops:
            wrong_coltype = optype not in [FLOAT_COLUMN, FLOAT_COLUMN_VIEW]
            if wrong_coltype:
                raise TypeError("This operator can only be applied to a FloatColumn!")

        if (
            oper in special_ops
            and oper != "boolean_as_num"
            and oper != "num_subselection"
        ):
            wrong_coltype = optype not in [STRING_COLUMN, STRING_COLUMN_VIEW]
            if wrong_coltype:
                raise TypeError("This operator can only be applied to a StringColumn!")

        if oper == "boolean_as_num" and optype != BOOLEAN_COLUMN_VIEW:
            raise TypeError("This operator can only be applied to a BooleanColumn!")

        if oper == "num_subselection":
            wrong_coltype = optype not in [
                STRING_COLUMN,
                STRING_COLUMN_VIEW,
                BOOLEAN_COLUMN_VIEW,
                FLOAT_COLUMN,
                FLOAT_COLUMN_VIEW,
            ]
            if wrong_coltype:
                raise TypeError(
                    "The subselection operator can only be applied to FloatColumn!"
                )

        return operand.cmd


# -----------------------------------------------------------------------------

FloatOperandType = Union[numbers.Real, np.datetime64, FloatColumn, FloatColumnView]
StringOperandType = Union[str, StringColumn, StringColumnView]

OperandType = Union[FloatOperandType, StringOperandType, BooleanColumnView]

# -----------------------------------------------------------------------------


def _abs(self):
    """Compute absolute value."""
    return FloatColumnView(operator="abs", operand1=self, operand2=None)


FloatColumn.abs = _abs  # type: ignore
FloatColumnView.abs = _abs  # type: ignore

# -----------------------------------------------------------------------------


def _acos(self):
    """Compute arc cosine."""
    return FloatColumnView(operator="acos", operand1=self, operand2=None)


FloatColumn.acos = _acos  # type: ignore
FloatColumnView.acos = _acos  # type: ignore


# -----------------------------------------------------------------------------


def _add(self, other: OperandType):
    if isinstance(other, (StringColumn, StringColumnView, str)):
        return self.as_str() + other

    return FloatColumnView(operator="plus", operand1=self, operand2=other)


def _radd(self, other: OperandType):
    if isinstance(other, (StringColumn, StringColumnView, str)):
        return other + self.as_str()

    return FloatColumnView(operator="plus", operand1=other, operand2=self)


FloatColumn.__add__ = _add  # type: ignore
FloatColumn.__radd__ = _radd  # type: ignore

FloatColumnView.__add__ = _add  # type: ignore
FloatColumnView.__radd__ = _radd  # type: ignore


# -----------------------------------------------------------------------------


def _assert_equal(self, alias: str = "new_column"):
    """
    ASSERT EQUAL aggregation.

    Throws an exception unless all values inserted
    into the aggregation are equal.

    Args:
        alias: Name for the new column.
    """
    return Aggregation(alias, self, "assert_equal").get()


FloatColumn.assert_equal = _assert_equal  # type: ignore
FloatColumnView.assert_equal = _assert_equal  # type: ignore


# -----------------------------------------------------------------------------


def _asin(self):
    """Compute arc sine."""
    return FloatColumnView(operator="asin", operand1=self, operand2=None)


FloatColumn.asin = _asin  # type: ignore
FloatColumnView.asin = _asin  # type: ignore

# -----------------------------------------------------------------------------


def _atan(self):
    """Compute arc tangent."""
    return FloatColumnView(operator="atan", operand1=self, operand2=None)


FloatColumn.atan = _atan  # type: ignore
FloatColumnView.atan = _atan  # type: ignore

# -----------------------------------------------------------------------------


def _avg(self, alias: str = "new_column"):
    """
    AVG aggregation.

    Args:
        alias: Name for the new column.
    """
    return Aggregation(alias, self, "avg").get()


FloatColumn.avg = _avg  # type: ignore
FloatColumnView.avg = _avg  # type: ignore

# -----------------------------------------------------------------------------


def _cbrt(self):
    """Compute cube root."""
    return FloatColumnView(operator="cbrt", operand1=self, operand2=None)


FloatColumn.cbrt = _cbrt  # type: ignore
FloatColumnView.cbrt = _cbrt  # type: ignore


# -----------------------------------------------------------------------------


def _ceil(self):
    """Round up value."""
    return FloatColumnView(operator="ceil", operand1=self, operand2=None)


FloatColumn.ceil = _ceil  # type: ignore
FloatColumnView.ceil = _ceil  # type: ignore


#  -----------------------------------------------------------------------------

FloatColumnView._collect_footer_data = _collect_footer_data  # type: ignore
BooleanColumnView._collect_footer_data = _collect_footer_data  # type: ignore
StringColumnView._collect_footer_data = _collect_footer_data  # type: ignore
FloatColumn._collect_footer_data = _collect_footer_data  # type: ignore
StringColumn._collect_footer_data = _collect_footer_data  # type: ignore

# -----------------------------------------------------------------------------


def _to_str(col: Any):
    if isinstance(col, (StringColumn, StringColumnView)):
        return col

    if isinstance(col, (FloatColumn, FloatColumnView)):
        return col.as_str()  # type: ignore

    return str(col)


def _concat(self, other: Any):
    return StringColumnView(
        operator="concat",
        operand1=self,
        operand2=_to_str(other),
    )


def _rconcat(self, other: Any):
    return StringColumnView(
        operator="concat",
        operand1=_to_str(other),
        operand2=self,
    )


StringColumn.__add__ = _concat  # type: ignore
StringColumn.__radd__ = _rconcat  # type: ignore

StringColumnView.__add__ = _concat  # type: ignore
StringColumnView.__radd__ = _rconcat  # type: ignore

# -----------------------------------------------------------------------------


def _check_inferred_time_stamps(self):
    inferred_name = _infer_column_name_recursive(self.cmd)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            # HACK: We need to create a dummy DataFrame to infer the schema with the right
            # time format
            inference_batch = pd.DataFrame(
                {inferred_name: pd.to_datetime(self[:10].to_numpy())}
            )
    except (ValueError, pd._libs.tslibs.parsing.DateParseError):
        pass
    else:
        schema = pa.Schema.from_pandas(inference_batch)
        # just called for emmitting the timezone related warnings
        postprocess_arrow_schema(
            schema, roles=Roles.from_dict({roles.time_stamp: [inferred_name]})
        )
        return


StringColumn._check_inferred_time_stamps = _check_inferred_time_stamps  # type: ignore
StringColumnView._check_inferred_time_stamps = _check_inferred_time_stamps  # type: ignore

# -----------------------------------------------------------------------------


def _contains(self, other: StringOperandType):
    """
    Returns a boolean column indicating whether a
    string or column entry is contained in the corresponding
    entry of the other column.
    """
    return BooleanColumnView(
        operator="contains",
        operand1=self,
        operand2=other,
    )


StringColumn.contains = _contains  # type: ignore
StringColumnView.contains = _contains  # type: ignore

# -----------------------------------------------------------------------------


def _cos(self):
    """Compute cosine."""
    return FloatColumnView(operator="cos", operand1=self, operand2=None)


FloatColumn.cos = _cos  # type: ignore
FloatColumnView.cos = _cos  # type: ignore

# -----------------------------------------------------------------------------


def _count(self, alias: str = "new_column"):
    """
    COUNT aggregation.

    Args:
        alias: Name for the new column.
    """
    return Aggregation(alias, self, "count").get()


FloatColumn.count = _count  # type: ignore
FloatColumnView.count = _count  # type: ignore

# -----------------------------------------------------------------------------


def _count_categorical(self, alias: str = "new_column"):
    """
    COUNT aggregation.

    Args:
        alias: Name for the new column.
    """
    return Aggregation(alias, self, "count_categorical").get()


StringColumn.count = _count_categorical  # type: ignore
StringColumnView.count = _count_categorical  # type: ignore

# -----------------------------------------------------------------------------


def _count_distinct(self, alias: str = "new_column"):
    """
    COUNT DISTINCT aggregation.

    Args:
        alias: Name for the new column.
    """
    return Aggregation(alias, self, "count_distinct").get()


StringColumn.count_distinct = _count_distinct  # type: ignore
StringColumnView.count_distinct = _count_distinct  # type: ignore

# -----------------------------------------------------------------------------


def _day(self):
    """Extract day (of the month) from a time stamp.

    If the column is numerical, that number will be interpreted as the
    number of days since epoch time (January 1, 1970).

    """
    return FloatColumnView(operator="day", operand1=self, operand2=None)


FloatColumn.day = _day  # type: ignore
FloatColumnView.day = _day  # type: ignore

# -----------------------------------------------------------------------------


def _eq_num(self, other: FloatOperandType):
    if not isinstance(other, op_types := get_args(FloatOperandType)):
        raise TypeError(
            TYPE_MISSMATCH_ERROR_MSG_TEMPLATE.format(
                col_type=f"{type(self)!r}",
                operand_types=", ".join(f"{op_type!r}" for op_type in op_types),
            )
        )
    return BooleanColumnView(
        operator="num_equal_to",
        operand1=self,
        operand2=other,
    )


def _eq_str(self, other: StringOperandType):
    if not isinstance(other, op_types := get_args(StringOperandType)):
        msg = TYPE_MISSMATCH_ERROR_MSG_TEMPLATE.format(
            col_type=f"{type(self)!r}",
            operand_types=", ".join(f"{op_type!r}" for op_type in op_types),
        )
        if getattr(self, "role", None) == roles.join_key:
            msg += "\n\nHint: join_keys are always StringColumn(View)s."
        raise TypeError(msg)
    return BooleanColumnView(
        operator="str_equal_to",
        operand1=self,
        operand2=other,
    )


FloatColumn.__eq__ = _eq_num  # type: ignore
FloatColumnView.__eq__ = _eq_num  # type: ignore

StringColumn.__eq__ = _eq_str  # type: ignore
StringColumnView.__eq__ = _eq_str  # type: ignore

# -----------------------------------------------------------------------------


def _erf(self):
    """Compute error function."""
    return FloatColumnView(operator="erf", operand1=self, operand2=None)


FloatColumn.erf = _erf  # type: ignore
FloatColumnView.erf = _erf  # type: ignore

# -----------------------------------------------------------------------------


def _exp(self):
    """Compute exponential function."""
    return FloatColumnView(operator="exp", operand1=self, operand2=None)


FloatColumn.exp = _exp  # type: ignore
FloatColumnView.exp = _exp  # type: ignore

# -----------------------------------------------------------------------------


def _floor(self):
    """Round down value."""
    return FloatColumnView(operator="floor", operand1=self, operand2=None)


FloatColumn.floor = _floor  # type: ignore
FloatColumnView.floor = _floor  # type: ignore

# -----------------------------------------------------------------------------

FloatColumnView._format = _format  # type: ignore
BooleanColumnView._format = _format  # type: ignore
StringColumnView._format = _format  # type: ignore
FloatColumn._format = _format  # type: ignore
StringColumn._format = _format  # type: ignore

# -----------------------------------------------------------------------------


def _gamma(self):
    """Compute gamma function."""
    return FloatColumnView(
        operator="tgamma",
        operand1=self,
        operand2=None,
    )


FloatColumn.gamma = _gamma  # type: ignore
FloatColumnView.gamma = _gamma  # type: ignore

# -----------------------------------------------------------------------------


def _ge(self, other: Any):
    return BooleanColumnView(
        operator="greater_equal",
        operand1=self,
        operand2=other,
    )


FloatColumn.__ge__ = _ge  # type: ignore
FloatColumnView.__ge__ = _ge  # type: ignore

# -----------------------------------------------------------------------------


def _gt(self, other):
    return BooleanColumnView(
        operator="greater",
        operand1=self,
        operand2=other,
    )


FloatColumn.__gt__ = _gt  # type: ignore
FloatColumnView.__gt__ = _gt  # type: ignore

# -----------------------------------------------------------------------------


def _hour(self):
    """Extract hour (of the day) from a time stamp.

    If the column is numerical, that number will be interpreted as the
    number of days since epoch time (January 1, 1970).

    """
    return FloatColumnView(operator="hour", operand1=self, operand2=None)


FloatColumn.hour = _hour  # type: ignore
FloatColumnView.hour = _hour  # type: ignore

# -----------------------------------------------------------------------------


def _is_inf(self):
    """Determine whether the value is infinite."""
    return BooleanColumnView(
        operator="is_inf",
        operand1=self,
        operand2=None,
    )


FloatColumn.is_inf = _is_inf  # type: ignore
FloatColumnView.is_inf = _is_inf  # type: ignore

# -----------------------------------------------------------------------------


def _is_nan(self):
    """Determine whether the value is nan."""
    return BooleanColumnView(
        operator="is_nan",
        operand1=self,
        operand2=None,
    )


FloatColumn.is_nan = _is_nan  # type: ignore
FloatColumnView.is_nan = _is_nan  # type: ignore

FloatColumn.is_null = _is_nan  # type: ignore
FloatColumnView.is_null = _is_nan  # type: ignore

# -----------------------------------------------------------------------------


def _is_null(self):
    """Determine whether the value is NULL."""
    return BooleanColumnView(
        operator="is_null",
        operand1=self,
        operand2=None,
    )


StringColumn.is_null = _is_null  # type: ignore
StringColumnView.is_null = _is_null  # type: ignore

# -----------------------------------------------------------------------------

FloatColumn.last_change = _last_change_from_col  # type: ignore
FloatColumnView.last_change = _last_change_from_col  # type: ignore
StringColumn.last_change = _last_change_from_col  # type: ignore
StringColumnView.last_change = _last_change_from_col  # type: ignore
BooleanColumnView.last_change = _last_change_from_col  # type: ignore

# -----------------------------------------------------------------------------

BooleanColumnView.__iter__ = _make_iter(bool)  # type: ignore
FloatColumnView.__iter__ = _make_iter(np.float64)  # type: ignore
StringColumnView.__iter__ = _make_iter(str)  # type: ignore
FloatColumn.__iter__ = _make_iter(np.float64)  # type: ignore
StringColumn.__iter__ = _make_iter(str)  # type: ignore

# -----------------------------------------------------------------------------


def _le(self, other):
    return BooleanColumnView(
        operator="less_equal",
        operand1=self,
        operand2=other,
    )


FloatColumn.__le__ = _le  # type: ignore
FloatColumnView.__le__ = _le  # type: ignore

# -----------------------------------------------------------------------------

BooleanColumnView.__len__ = _length  # type: ignore
FloatColumnView.__len__ = _length  # type: ignore
StringColumnView.__len__ = _length  # type: ignore
FloatColumn.__len__ = _length  # type: ignore
StringColumn.__len__ = _length  # type: ignore

#  -----------------------------------------------------------------------------

BooleanColumnView.length = _length_property  # type: ignore
FloatColumnView.length = _length_property  # type: ignore
StringColumnView.length = _length_property  # type: ignore
FloatColumn.length = _length_property  # type: ignore
StringColumn.length = _length_property  # type: ignore

#  -----------------------------------------------------------------------------


def _lgamma(self):
    """Compute log-gamma function."""
    return FloatColumnView(
        operator="lgamma",
        operand1=self,
        operand2=None,
    )


FloatColumn.lgamma = _lgamma  # type: ignore
FloatColumnView.lgamma = _lgamma  # type: ignore

# -----------------------------------------------------------------------------


def _log(self):
    """Compute natural logarithm."""
    return FloatColumnView(operator="log", operand1=self, operand2=None)


FloatColumn.log = _log  # type: ignore
FloatColumnView.log = _log  # type: ignore

# -----------------------------------------------------------------------------


def _lt(self, other):
    return BooleanColumnView(operator="less", operand1=self, operand2=other)


FloatColumn.__lt__ = _lt  # type: ignore
FloatColumnView.__lt__ = _lt  # type: ignore

# -----------------------------------------------------------------------------


def _max(self, alias: str = "new_column"):
    """
    MAX aggregation.

    Args:
        alias: Name for the new column.
    """
    return Aggregation(alias, self, "max").get()


FloatColumn.max = _max  # type: ignore
FloatColumnView.max = _max  # type: ignore

# -----------------------------------------------------------------------------


def _median(self, alias: str = "new_column"):
    """
    MEDIAN aggregation.

    Args:
        alias: Name for the new column.
    """
    return Aggregation(alias, self, "median").get()


FloatColumn.median = _median  # type: ignore
FloatColumnView.median = _median  # type: ignore

# -----------------------------------------------------------------------------


def _min(self, alias="new_column"):
    """
    MIN aggregation.

    **alias**: Name for the new column.
    """
    return Aggregation(alias, self, "min").get()


FloatColumn.min = _min  # type: ignore
FloatColumnView.min = _min  # type: ignore

# -----------------------------------------------------------------------------


def _minute(self):
    """Extract minute (of the hour) from a time stamp.

    If the column is numerical, that number will be interpreted as the
    number of days since epoch time (January 1, 1970).

    """
    return FloatColumnView(
        operator="minute",
        operand1=self,
        operand2=None,
    )


FloatColumn.minute = _minute  # type: ignore
FloatColumnView.minute = _minute  # type: ignore

# -----------------------------------------------------------------------------


def _mod(self, other):
    return FloatColumnView(operator="fmod", operand1=self, operand2=other)


def _rmod(self, other):
    return FloatColumnView(operator="fmod", operand1=other, operand2=self)


FloatColumn.__mod__ = _mod  # type: ignore
FloatColumn.__rmod__ = _rmod  # type: ignore

FloatColumnView.__mod__ = _mod  # type: ignore
FloatColumnView.__rmod__ = _rmod  # type: ignore

# -----------------------------------------------------------------------------


def _month(self):
    """
    Extract month from a time stamp.

    If the column is numerical, that number will be interpreted
    as the number of days since epoch time (January 1, 1970).
    """
    return FloatColumnView(operator="month", operand1=self, operand2=None)


FloatColumn.month = _month  # type: ignore
FloatColumnView.month = _month  # type: ignore

# -----------------------------------------------------------------------------


def _mul(self, other):
    return FloatColumnView(
        operator="multiplies",
        operand1=self,
        operand2=other,
    )


FloatColumn.__mul__ = _mul  # type: ignore
FloatColumn.__rmul__ = _mul  # type: ignore

FloatColumnView.__mul__ = _mul  # type: ignore
FloatColumnView.__rmul__ = _mul  # type: ignore

# -----------------------------------------------------------------------------


def _ne_num(self, other):
    return BooleanColumnView(
        operator="num_not_equal_to",
        operand1=self,
        operand2=other,
    )


def _ne_str(self, other):
    return BooleanColumnView(
        operator="str_not_equal_to",
        operand1=self,
        operand2=other,
    )


FloatColumn.__ne__ = _ne_num  # type: ignore
FloatColumnView.__ne__ = _ne_num  # type: ignore

StringColumn.__ne__ = _ne_str  # type: ignore
StringColumnView.__ne__ = _ne_str  # type: ignore

# -----------------------------------------------------------------------------


def _neg(self):
    return FloatColumnView(
        operator="multiplies",
        operand1=self,
        operand2=-1.0,
    )


FloatColumn.__neg__ = _neg  # type: ignore
FloatColumnView.__neg__ = _neg  # type: ignore

# -----------------------------------------------------------------------------


def _pow(self, other: FloatOperandType):
    return FloatColumnView(operator="pow", operand1=self, operand2=other)


def _rpow(self, other: FloatOperandType):
    return FloatColumnView(operator="pow", operand1=other, operand2=self)


FloatColumn.__pow__ = _pow  # type: ignore
FloatColumn.__rpow__ = _rpow  # type: ignore

FloatColumnView.__pow__ = _pow  # type: ignore
FloatColumnView.__rpow__ = _rpow  # type: ignore

# -----------------------------------------------------------------------------

FloatColumn.__repr__ = _repr  # type: ignore
FloatColumnView.__repr__ = _repr  # type: ignore
StringColumn.__repr__ = _repr  # type: ignore
StringColumnView.__repr__ = _repr  # type: ignore
BooleanColumnView.__repr__ = _repr  # type: ignore

# -----------------------------------------------------------------------------

FloatColumn._repr_html_ = _repr_html  # type: ignore
FloatColumnView._repr_html_ = _repr_html  # type: ignore
StringColumn._repr_html_ = _repr_html  # type: ignore
StringColumnView._repr_html_ = _repr_html  # type: ignore
BooleanColumnView._repr_html_ = _repr_html  # type: ignore

# -----------------------------------------------------------------------------


def _round(self):
    """Round to nearest."""
    return FloatColumnView(operator="round", operand1=self, operand2=None)


FloatColumn.round = _round  # type: ignore
FloatColumnView.round = _round  # type: ignore

# -----------------------------------------------------------------------------


def _second(self):
    """Extract second (of the minute) from a time stamp.

    If the column is numerical, that number will be interpreted as the
    number of days since epoch time (January 1, 1970).

    """
    return FloatColumnView(
        operator="second",
        operand1=self,
        operand2=None,
    )


FloatColumn.second = _second  # type: ignore
FloatColumnView.second = _second  # type: ignore

# -----------------------------------------------------------------------------


def _sin(self):
    """Compute sine."""
    return FloatColumnView(operator="sin", operand1=self, operand2=None)


FloatColumn.sin = _sin  # type: ignore
FloatColumnView.sin = _sin  # type: ignore

# -----------------------------------------------------------------------------


def _sqrt(self):
    """Compute square root."""
    return FloatColumnView(operator="sqrt", operand1=self, operand2=None)


FloatColumn.sqrt = _sqrt  # type: ignore
FloatColumnView.sqrt = _sqrt  # type: ignore

# -----------------------------------------------------------------------------


def _stddev(self, alias: str = "new_column"):
    """
    STDDEV aggregation.

    Args:
        alias: Name for the new column.
    """
    return Aggregation(alias, self, "stddev").get()


FloatColumn.stddev = _stddev  # type: ignore
FloatColumnView.stddev = _stddev  # type: ignore

# -----------------------------------------------------------------------------


def _sub(self, other):
    return FloatColumnView(
        operator="minus",
        operand1=self,
        operand2=other,
    )


def _rsub(self, other):
    return FloatColumnView(
        operator="minus",
        operand1=other,
        operand2=self,
    )


FloatColumn.__sub__ = _sub  # type: ignore
FloatColumn.__rsub__ = _rsub  # type: ignore

FloatColumnView.__sub__ = _sub  # type: ignore
FloatColumnView.__rsub__ = _rsub  # type: ignore

# -------------------------------------------------------------------------

FloatColumnView.subroles = _subroles  # type: ignore
StringColumnView.subroles = _subroles  # type: ignore
FloatColumn.subroles = _subroles  # type: ignore
StringColumn.subroles = _subroles  # type: ignore

# -----------------------------------------------------------------------------


def _subselection_bool(self, indices):
    if isinstance(indices, numbers.Integral):
        return _get_scalar(self, indices)

    if isinstance(indices, slice):
        indices = _make_slicing_operand(self, indices)

    return BooleanColumnView(
        operator="bool_subselection",
        operand1=self,
        operand2=indices,
    )


BooleanColumnView.__getitem__ = _subselection_bool  # type: ignore

# -----------------------------------------------------------------------------


def _subselection_float(self, indices):
    if isinstance(indices, numbers.Integral):
        return _get_scalar(self, indices)

    if isinstance(indices, slice):
        indices = _make_slicing_operand(self, indices)

    return FloatColumnView(
        operator="num_subselection",
        operand1=self,
        operand2=indices,
    )


FloatColumnView.__getitem__ = _subselection_float  # type: ignore
FloatColumn.__getitem__ = _subselection_float  # type: ignore

# -----------------------------------------------------------------------------


def _subselection_string(self, indices):
    if isinstance(indices, numbers.Integral):
        return _get_scalar(self, indices)

    if isinstance(indices, slice):
        indices = _make_slicing_operand(self, indices)

    return StringColumnView(
        operator="str_subselection",
        operand1=self,
        operand2=indices,
    )


StringColumnView.__getitem__ = _subselection_string  # type: ignore
StringColumn.__getitem__ = _subselection_string  # type: ignore

# -----------------------------------------------------------------------------


def _substr(self, begin: int, length: int):
    """
    Return a substring for every element in the column.

    Args:
        begin: First position of the original string.
        length: Length of the extracted string.
    """
    col = StringColumnView(
        operator="substr",
        operand1=self,
        operand2=None,
    )
    col.cmd["begin_"] = begin
    col.cmd["len_"] = length
    return col


StringColumn.substr = _substr  # type: ignore
StringColumnView.substr = _substr  # type: ignore

# -----------------------------------------------------------------------------


def _sum(self, alias: str = "new_column"):
    """
    SUM aggregation.

    Args:
        alias: Name for the new column.
    """
    return Aggregation(alias, self, "sum").get()


FloatColumn.sum = _sum  # type: ignore
FloatColumnView.sum = _sum  # type: ignore

# -----------------------------------------------------------------------------


def _tan(self):
    """Compute tangent."""
    return FloatColumnView(operator="tan", operand1=self, operand2=None)


FloatColumn.tan = _tan  # type: ignore
FloatColumnView.tan = _tan  # type: ignore

# -----------------------------------------------------------------------------


def _as_num(self):
    """Transforms a categorical column to a numerical column."""
    return FloatColumnView(
        operator="as_num",
        operand1=self,
        operand2=None,
    )


StringColumn.as_num = _as_num  # type: ignore
StringColumnView.as_num = _as_num  # type: ignore

# -----------------------------------------------------------------------------

BooleanColumnView.to_numpy = _to_numpy  # type: ignore
FloatColumn.to_numpy = _to_numpy  # type: ignore
FloatColumnView.to_numpy = _to_numpy  # type: ignore
StringColumn.to_numpy = _to_numpy  # type: ignore
StringColumnView.to_numpy = _to_numpy  # type: ignore

# -----------------------------------------------------------------------------


def _as_str(self):
    """Transforms column to a string."""
    return StringColumnView(
        operator="as_str",
        operand1=self,
        operand2=None,
    )


FloatColumn.as_str = _as_str  # type: ignore
FloatColumnView.as_str = _as_str  # type: ignore

BooleanColumnView.as_str = _as_str  # type: ignore

# -----------------------------------------------------------------------------


def _as_ts(self, time_formats: Optional[Union[str, Iterable[str]]] = None):
    """
    Transforms a categorical column to a time stamp.

    Args:
        time_formats: Formats to be used to parse the time stamps.
    """
    inferred_name = _infer_column_name_recursive(self.cmd)
    self._check_inferred_time_stamps()
    time_formats = time_formats or TIME_FORMATS
    col = FloatColumnView(operator="as_ts", operand1=self, operand2=None)
    col.cmd["time_formats_"] = time_formats

    if all(col[:10].is_nan().to_numpy()):
        warnings.warn(
            TIME_STAMP_PARSING_LIKELY_FAILED_WARNING_TEMPLATE.format(
                column=inferred_name
            )
        )
    return col


StringColumn.as_ts = _as_ts  # type: ignore
StringColumnView.as_ts = _as_ts  # type: ignore

# -----------------------------------------------------------------------------


def _truediv(self, other: FloatOperandType):
    return FloatColumnView(
        operator="divides",
        operand1=self,
        operand2=other,
    )


def _rtruediv(self, other: FloatOperandType):
    return FloatColumnView(
        operator="divides",
        operand1=other,
        operand2=self,
    )


FloatColumn.__truediv__ = _truediv  # type: ignore
FloatColumn.__rtruediv__ = _rtruediv  # type: ignore

FloatColumnView.__truediv__ = _truediv  # type: ignore
FloatColumnView.__rtruediv__ = _rtruediv  # type: ignore

# -----------------------------------------------------------------------------

FloatColumn.unique = _unique  # type: ignore
FloatColumnView.unique = _unique  # type: ignore
StringColumn.unique = _unique  # type: ignore
StringColumnView.unique = _unique  # type: ignore

# -------------------------------------------------------------------------

FloatColumnView.unit = _unit  # type: ignore
StringColumnView.unit = _unit  # type: ignore
FloatColumn.unit = _unit  # type: ignore
StringColumn.unit = _unit  # type: ignore

# -----------------------------------------------------------------------------


def _update_float(self, condition: BooleanColumnView, values: FloatOperandType):
    """
    Returns an updated version of this column.

    All entries for which the corresponding **condition** is True,
    are updated using the corresponding entry in **values**.

    Args:
        condition: Condition according to which the update is done
        values: Values to update with
    """
    col = FloatColumnView(
        operator="num_update",
        operand1=self,
        operand2=values,
    )
    if condition.cmd["type_"] != BOOLEAN_COLUMN_VIEW:
        raise TypeError("Condition for an update must be a Boolean column.")
    col.cmd["condition_"] = condition.cmd
    return col


FloatColumn.update = _update_float  # type: ignore

FloatColumnView.update = _update_float  # type: ignore

# -----------------------------------------------------------------------------


def _update_str(self, condition: BooleanColumnView, values: StringOperandType):
    """
    Returns an updated version of this column.

    All entries for which the corresponding **condition** is True,
    are updated using the corresponding entry in **values**.

    Args:
        condition: Condition according to which the update is done
        values: Values to update with
    """
    col = StringColumnView(
        operator="str_update",
        operand1=self,
        operand2=values,
    )
    if condition.cmd["type_"] != BOOLEAN_COLUMN_VIEW:
        raise TypeError("Condition for an update must be a Boolean column.")
    col.cmd["condition_"] = condition.cmd
    return col


StringColumn.update = _update_str  # type: ignore

StringColumnView.update = _update_str  # type: ignore

# -----------------------------------------------------------------------------


def _var(self, alias: str = "new_column"):
    """
    VAR aggregation.

    Args:
        alias: Name for the new column.
    """
    return Aggregation(alias, self, "var").get()


FloatColumn.var = _var  # type: ignore
FloatColumnView.var = _var  # type: ignore

# -----------------------------------------------------------------------------


def _weekday(self):
    """Extract day of the week from a time stamp, Sunday being 0.

    If the column is numerical, that number will be interpreted as the
    number of days since epoch time (January 1, 1970).

    """
    return FloatColumnView(
        operator="weekday",
        operand1=self,
        operand2=None,
    )


FloatColumn.weekday = _weekday  # type: ignore
FloatColumnView.weekday = _weekday  # type: ignore

# -----------------------------------------------------------------------------


def _with_subroles_float(self, subroles: Union[str, List[str]], append: bool = True):
    """
    Returns a new column with new subroles.

    Args:
        subroles: The subroles to be assigned.

        append: Whether you want to append the
            new subroles to the existing subroles.
    """
    if isinstance(subroles, str):
        subroles = [subroles]

    if not isinstance(subroles, list):
        raise TypeError("'subroles' must be a str or a list of str.")

    if not isinstance(append, bool):
        raise TypeError("'append' must be a bool.")

    col = FloatColumnView(
        operator="num_with_subroles",
        operand1=self,
        operand2=None,
    )

    col.cmd["subroles_"] = self.subroles + subroles if append else subroles

    return col


FloatColumn.with_subroles = _with_subroles_float  # type: ignore
FloatColumnView.with_subroles = _with_subroles_float  # type: ignore


# -----------------------------------------------------------------------------


def _with_subroles_string(self, subroles: Union[str, List[str]], append: bool = True):
    """
    Returns a new column with new subroles.

    Args:
        subroles: The subroles to be assigned.

        append: Whether you want to append the
            new subroles to the existing subroles.
    """
    if isinstance(subroles, str):
        subroles = [subroles]

    if not isinstance(subroles, list):
        raise TypeError("'subroles' must be a str or a list of str.")

    if not isinstance(append, bool):
        raise TypeError("'append' must be a bool.")

    col = StringColumnView(
        operator="str_with_subroles",
        operand1=self,
        operand2=None,
    )

    col.cmd["subroles_"] = self.subroles + subroles if append else subroles

    return col


StringColumn.with_subroles = _with_subroles_string  # type: ignore
StringColumnView.with_subroles = _with_subroles_string  # type: ignore

# -----------------------------------------------------------------------------


def _with_unit_float(self, unit: str):
    """
    Returns a new column with a new unit.

    Args:
        unit: The new unit.
    """
    col = FloatColumnView(
        operator="num_with_unit",
        operand1=self,
        operand2=None,
    )
    col.cmd["unit_"] = unit
    return col


FloatColumn.with_unit = _with_unit_float  # type: ignore
FloatColumnView.with_unit = _with_unit_float  # type: ignore


# -----------------------------------------------------------------------------


def _with_unit_string(self, unit: str):
    """
    Returns a new column with a new unit,

    Args:
        unit: The new unit.
    """
    col = StringColumnView(
        operator="str_with_unit",
        operand1=self,
        operand2=None,
    )
    col.cmd["unit_"] = unit
    return col


StringColumn.with_unit = _with_unit_string  # type: ignore
StringColumnView.with_unit = _with_unit_string  # type: ignore

# -----------------------------------------------------------------------------


def _year(self):
    """
    Extract year from a time stamp.

    If the column is numerical, that number will be interpreted
    as the number of days since epoch time (January 1, 1970).
    """
    return FloatColumnView(operator="year", operand1=self, operand2=None)


FloatColumn.year = _year  # type: ignore
FloatColumnView.year = _year  # type: ignore

# -----------------------------------------------------------------------------


def _yearday(self):
    """
    Extract day of the year from a time stamp.

    If the column is numerical, that number will be interpreted
    as the number of days since epoch time (January 1, 1970).
    """
    return FloatColumnView(
        operator="yearday",
        operand1=self,
        operand2=None,
    )


FloatColumn.yearday = _yearday  # type: ignore
FloatColumnView.yearday = _yearday  # type: ignore


# -----------------------------------------------------------------------------
