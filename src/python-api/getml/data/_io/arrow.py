# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""
Utilities for handling Arrow data structures.
"""

from __future__ import annotations

import re
import warnings
from dataclasses import dataclass, field
from functools import wraps
from inspect import cleandoc
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Iterator, List, Union

import pyarrow as pa
from pyarrow.lib import ArrowInvalid

from getml import communication as comm
from getml.data.roles import sets as roles_sets
from getml.data.roles.container import Roles
from getml.data.roles.types import Role
from getml.exceptions import arrow_cast_exception_handler

if TYPE_CHECKING:
    from getml.data.data_frame import DataFrame

MAX_IEEE754_COMPAT_INT = 2**53
MIN_IEEE754_COMPAT_INT = -MAX_IEEE754_COMPAT_INT
"""
IEEE 754 double-precision numbers have a 53-bit mantissa, allowing exact
representation of integers up to 2**53. The minimum exact integer
representable is the negative of this maximum value.
"""

INT_ENCODING_HINTS_PARTIAL = cleandoc(
    """
    If your column contains catergorical values, you can either cast the column to
    string before sending the data to the getML engine or explicitly set the
    column's role to 'categorical' in `from_...` or `read_...` methods.

    If your column contains numerical values, rescale your integers to be within the
    range of integers representable by IEEE754 double precision 64bit floating
    point (float64) numbers:
    [{min_ieee754_compat_int}, {max_ieee754_compat_int}].
    """
).format(
    min_ieee754_compat_int=MIN_IEEE754_COMPAT_INT,
    max_ieee754_compat_int=MAX_IEEE754_COMPAT_INT,
)


DECIMAL_CONVERSION_LOST_PRECISION_WARNING_TEMPLATE = cleandoc(
    """
    \nColumn '{column_name}' has been converted from decimal to float. This may
    result in a loss of precision!
    """
)

INT64_EXCEEDS_FLOAT64_BOUNDS_ERROR_TEMPLATE = cleandoc(
    """
    Column '{{column_name}}' contains values that exceed the bounds of a float64. getML
    uses double precision floating point numbers to represent numerical values.

    {int_encoding_hints}
    """
).format(int_encoding_hints=INT_ENCODING_HINTS_PARTIAL)

TIMESTAMP_ENCODING_HINTS_PARTIAL = cleandoc(
    """
    Currently, getML doesn't support the handling of explicit timezones for
    timestamps. If you need timezone information to be available for your models, you
    can encode the timezone explicitly in a separate column.
    """
)

TIMEZONE_UTC_DROPPED_WARNING_TEMPLATE = cleandoc(
    """
    \nColumn '{{column_name}}' has UTC timezone. Dropping the timezone.

    {timestamp_encoding_hints}
    """
).format(timestamp_encoding_hints=TIMESTAMP_ENCODING_HINTS_PARTIAL)

TIMEZONE_NON_UTC_ERROR_TEMPLATE = cleandoc(
    """
    Column '{{column_name}}' has a timezone '{{timezone}}' that is not UTC. Please
    cast the timestamp to be UTC-based or drop the timezone.

    {timestamp_encoding_hints}
    """
).format(timestamp_encoding_hints=TIMESTAMP_ENCODING_HINTS_PARTIAL)

UNSUPPORTED_TYPE_WARNING_TEMPLATE = cleandoc(
    """
    Column '{column_name}' has an unsupported type: {type}. The column will be ignored.
    """
)

UNPARAMETERIZED_TYPE_CONVERSION_MAPPING = {
    pa.int8(): pa.float64(),
    pa.int16(): pa.float64(),
    pa.int32(): pa.float64(),
    pa.int64(): pa.float64(),
    pa.uint8(): pa.float64(),
    pa.uint16(): pa.float64(),
    pa.uint32(): pa.float64(),
    pa.uint64(): pa.float64(),
    pa.float16(): pa.float64(),
    pa.float32(): pa.float64(),
}
"""
Mapping from unsupported native unparameterized arrow types to supported types
that can be handled by the getML engine.

Arrow's parameterized types (e.g. decimals) are not included here, as they are
parameterized and keeping track of all possible parameterizations would be cumbersome.
As such parameterized types are handled in a separate processor that dynamically
handles such types by checking against the pyarrows respective ABCs at runtime.
"""


@dataclass
class Processors:
    categorical: List[Callable[[pa.Field], pa.Field]] = field(default_factory=list)
    numerical: List[Callable[[pa.Field], pa.Field]] = field(default_factory=list)
    target: List[Callable[[pa.Field], pa.Field]] = field(default_factory=list)
    time_stamp: List[Callable[[pa.Field], pa.Field]] = field(default_factory=list)
    join_key: List[Callable[[pa.Field], pa.Field]] = field(default_factory=list)
    text: List[Callable[[pa.Field], pa.Field]] = field(default_factory=list)
    unused_float: List[Callable[[pa.Field], pa.Field]] = field(default_factory=list)
    unused_string: List[Callable[[pa.Field], pa.Field]] = field(default_factory=list)

    def __getitem__(self, role: Role) -> List[Callable[[pa.Field], pa.Field]]:
        return getattr(self, role)


class ArrowSchemaFieldProcessorRegistry:
    processors: Processors = Processors()

    def __call__(
        self, roles: Iterable[Role]
    ) -> Callable[
        [Callable[[pa.Field], None]],
        Callable[[pa.Field], pa.Field],
    ]:
        return self.register(roles)

    @classmethod
    def register(
        cls, roles: Iterable[Role]
    ) -> Callable[
        [Callable[[pa.Field], None]],
        Callable[[pa.Field], pa.Field],
    ]:
        def decorator(func: Callable[[pa.Field], pa.Field]):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            for role in roles:
                cls.processors[role].append(wrapper)
            return wrapper

        return decorator

    @classmethod
    def retrieve(cls, role: Role) -> List[Callable[[pa.Field], pa.Field]]:
        return cls.processors[role]


arrow_schema_field_processor = ArrowSchemaFieldProcessorRegistry()


def _is_numerical_type_arrow(coltype: pa.DataType) -> bool:
    return any(
        [
            pa.types.is_integer(coltype),
            pa.types.is_floating(coltype),
            pa.types.is_decimal(coltype),
        ]
    )


def _is_unsupported_type_arrow(coltype: pa.DataType) -> bool:
    return any(
        [
            pa.types.is_dictionary(coltype),
            pa.types.is_large_list(coltype),
            pa.types.is_list(coltype),
            pa.types.is_map(coltype),
            pa.types.is_struct(coltype),
            pa.types.is_union(coltype),
        ]
    )


@arrow_schema_field_processor(roles=roles_sets.all_)
def process_time_stamp(field: pa.Field) -> pa.Field:
    if pa.types.is_timestamp(field.type):
        if field.type.tz is None:
            return field.with_metadata({})
        elif field.type.tz == "UTC":
            warnings.warn(
                TIMEZONE_UTC_DROPPED_WARNING_TEMPLATE.format(column_name=field.name)
            )
            return pa.field(field.name, pa.timestamp(field.type.unit))
        else:
            raise TypeError(
                TIMEZONE_NON_UTC_ERROR_TEMPLATE.format(
                    column_name=field.name, timezone=field.type.tz
                )
            )
    else:
        return field.with_metadata({})


@arrow_schema_field_processor(roles=roles_sets.numerical)
def process_unparameterized_type(field: pa.Field) -> pa.Field:
    if field.type in UNPARAMETERIZED_TYPE_CONVERSION_MAPPING:
        return pa.field(
            field.name,
            UNPARAMETERIZED_TYPE_CONVERSION_MAPPING[field.type],
        )
    return field


@arrow_schema_field_processor(roles=roles_sets.all_)
def process_parameterized_type(field: pa.Field) -> pa.Field:
    if pa.types.is_decimal(field.type):
        warnings.warn(
            DECIMAL_CONVERSION_LOST_PRECISION_WARNING_TEMPLATE.format(
                column_name=field.name
            )
        )
        return pa.field(field.name, pa.float64())
    return field


def process_arrow_schema(schema: pa.Schema, roles: Roles) -> pa.Schema:
    processed_fields = []
    for name in roles.columns:
        role = roles.column(name)
        field = schema.field(name)
        field_processed = field.with_metadata({})
        for processor in arrow_schema_field_processor.retrieve(role):
            field_processed = processor(field_processed)
        processed_fields.append(field_processed)
    return pa.schema(processed_fields)


def sniff_arrow(table) -> Roles:
    return sniff_schema(table.schema)


def sniff_schema(schema: pa.Schema, colnames: Iterable[str] = ()) -> Roles:
    roles: Dict[Role, List[str]] = {}
    roles["unused_float"] = []
    roles["unused_string"] = []

    if not colnames:
        colnames = schema.names
    else:
        colnames = [cname for cname in colnames if cname in schema.names]

    coltypes = schema.types

    for cname, ctype in zip(colnames, coltypes):
        if _is_unsupported_type_arrow(ctype):
            warnings.warn(
                UNSUPPORTED_TYPE_WARNING_TEMPLATE.format(column_name=cname, type=ctype)
            )
            continue
        if _is_numerical_type_arrow(ctype):
            roles["unused_float"].append(cname)
        else:
            roles["unused_string"].append(cname)

    return Roles.from_dict(roles)


def to_arrow(df_or_view: Union[DataFrame, View]) -> pa.Table:
    df_or_view.refresh()

    cmd: Dict[str, Any] = {}

    typename = type(df_or_view).__name__

    cmd["type_"] = typename + ".to_arrow"
    cmd["name_"] = df_or_view.name if typename == "DataFrame" else ""

    if typename == "View":
        cmd["view_"] = df_or_view._getml_deserialize()

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.handle_engine_exception(msg)
        with sock.makefile(mode="rb") as stream:
            with pa.ipc.open_stream(stream) as reader:
                return reader.read_all()


def to_arrow_batches(
    batch_or_batches: Union[pa.RecordBatch, pa.Table, Iterable[pa.RecordBatch]],
) -> Iterator[pa.RecordBatch]:
    if isinstance(batch := batch_or_batches, pa.RecordBatch):
        return iter([batch])
    elif isinstance(batches := batch_or_batches, Iterable):
        if all(isinstance(batch, pa.RecordBatch) for batch in batches):
            return batches  # type: ignore
        else:
            raise TypeError(
                "If 'data' is an iterable, all elements must be of type pa.RecordBatch."
            )
    elif isinstance(table := batch_or_batches, pa.Table):
        return table.to_batches()  # type: ignore
    else:
        raise TypeError(
            "'data' must be a pa.RecordBatch, pa.Table, or an iterable of pa.RecordBatch instances."
        )


@arrow_cast_exception_handler(pa.float64())
def handle_float64_exc(exc: ArrowInvalid, field: pa.Field) -> None:
    if re.match(r"Integer value \d+ not in range", str(exc)):
        raise OverflowError(
            INT64_EXCEEDS_FLOAT64_BOUNDS_ERROR_TEMPLATE.format(column_name=field.name)
        ) from exc
    raise exc


def cast_arrow_batch(
    source_batch: Iterable[pa.RecordBatch], target_schema: pa.Schema
) -> Iterable[pa.RecordBatch]:
    """
    Cast a source record batch to be compliant with a target schema.
    """
    target_arrays = []
    for target_field in target_schema:
        source_array = source_batch.column(target_field.name)  # type: ignore
        if source_array.type != target_field.type:
            try:
                target_arrays.append(source_array.cast(target_field.type))
            except ArrowInvalid as e:
                handler = arrow_cast_exception_handler.retrieve(target_field.type)
                handler(e, target_field)
        else:
            target_arrays.append(source_array)
    return pa.RecordBatch.from_arrays(target_arrays, schema=target_schema)


def read_arrow_batches(
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    df: DataFrame,
    append: bool = False,
) -> None:
    cmd: Dict[str, Any] = {}

    cmd["type_"] = "DataFrame.from_arrow"
    cmd["name_"] = df.name

    cmd["append_"] = append

    cmd["categorical_"] = df._categorical_names
    cmd["join_keys_"] = df._join_key_names
    cmd["numerical_"] = df._numerical_names
    cmd["targets_"] = df._target_names
    cmd["text_"] = df._text_names
    cmd["time_stamps_"] = df._time_stamp_names
    cmd["unused_floats_"] = df._unused_float_names
    cmd["unused_strings_"] = df._unused_string_names

    processed_schema = process_arrow_schema(schema, df.roles)

    with comm.send_and_get_socket(cmd) as sock:
        with sock.makefile(mode="wb") as sink:
            with pa.ipc.new_stream(sink, processed_schema) as writer:
                for batch in batches:
                    if processed_schema != batch.schema:
                        compliant_batch = cast_arrow_batch(batch, processed_schema)
                    else:
                        compliant_batch = batch
                    writer.write_batch(compliant_batch)
        msg = comm.recv_string(sock)

    if msg != "Success!":
        comm.handle_engine_exception(msg)
