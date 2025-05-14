# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""
Utilities for handling Arrow data structures.
"""

from __future__ import annotations

import json
import re
import socket
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
from inspect import cleandoc
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Tuple,
    Union,
    cast,
)

import pyarrow as pa
import pyarrow.compute as pa_compute
from pyarrow.lib import ArrowInvalid

from getml import communication as comm
from getml.constants import DEFAULT_BATCH_SIZE
from getml.data.roles import roles
from getml.data.roles import sets as roles_sets
from getml.data.roles.container import Roles
from getml.data.roles.types import Role
from getml.exceptions import arrow_cast_exception_handler

if TYPE_CHECKING:
    from getml.data.data_frame import DataFrame
    from getml.data.view import View

MAX_SUPPORTED_TIMESTAMP_RESOLUTION = "us"
"""
Maximum timestamp resolution supported by getML.
"""

MAX_IEEE754_COMPATIBLE_INT = 2**53
MIN_IEEE754_COMPATIBLE_INT = -MAX_IEEE754_COMPATIBLE_INT
"""
IEEE 754 double-precision numbers have a 53-bit mantissa, allowing exact
representation of integers up to 2**53. The minimum exact integer
representable is the negative of this maximum value.
"""

INT_ENCODING_HINTS_PARTIAL = cleandoc(
    """
    If the column contains catergorical values, you can either cast the column to
    string before reading data into getML or explicitly set the column's role to
    'categorical' in `from_...` or `read_...` methods.

    If the column contains numerical values, rescale your integers to be within the
    range of integers representable by IEEE754 double precision 64bit floating
    point (float64) numbers:
    [{min_ieee754_compatible_int}, {max_ieee754_compatible_int}].
    """
).format(
    min_ieee754_compatible_int=MIN_IEEE754_COMPATIBLE_INT,
    max_ieee754_compatible_int=MAX_IEEE754_COMPATIBLE_INT,
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

UNSUPPORTED_TYPE_PREDICATES = (
    pa.types.is_dictionary,
    pa.types.is_large_list,
    pa.types.is_list,
    pa.types.is_map,
    pa.types.is_struct,
    pa.types.is_union,
)
"""
Arrow predicates identifying unsupported types.
"""

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
    pa.null(): pa.string(),
}
"""
Mapping from unsupported native unparameterized arrow types to supported types
that can be handled by the getML Engine.

Arrow's parameterized types (e.g. decimals) are not included here, as they are
parameterized and keeping track of all possible parameterizations would be cumbersome.
As such, parameterized types are handled in a separate processor that dynamically
handles such types by checking against the pyarrows respective ABCs at runtime.

As there is no dedicated null type in getML, nulls are cast to string.
"""


@dataclass
class Processors:
    """
    Container for arrow schema field processors. Each role is associated with a
    list of processors that are applied to fields associated with columns that
    are going to be treated as the respective role.
    """

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
    """
    Registry for arrow schema field processors.

    Instances can be used as a decorator to register a function as a
    processor for arrow schema fields. Processors are registered targeting
    a set of roles on the engine. If a field is associated with a column that
    is going to be treated as any of the registered roles, the processor
    will be applied to the respective field.
    """

    def __init__(self):
        self.processors: Processors = Processors()

    def __call__(
        self, roles: Iterable[Role]
    ) -> Callable[
        [Callable[[pa.Field], None]],
        Callable[[pa.Field], pa.Field],
    ]:
        return self.register(roles)

    def register(
        self, roles: Iterable[Role]
    ) -> Callable[
        [Callable[[pa.Field], None]],
        Callable[[pa.Field], pa.Field],
    ]:
        def decorator(func: Callable[[pa.Field], pa.Field]):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            for role in roles:
                self.processors[role].append(wrapper)
            return wrapper

        return decorator

    def retrieve(self, role: Role) -> List[Callable[[pa.Field], pa.Field]]:
        return self.processors[role]


arrow_schema_field_preprocessor = ArrowSchemaFieldProcessorRegistry()
"""
Registry for arrow schema field preprocessors.

Can be used as a decorator to register a function as a preprocessor for arrow
schema fields.

Preprocessors are applied _before_ any data has been read into arrow.

Each preprocessor is registered targeting set of roles. Preprocessors
are applied to fields that are associated with any of the registered roles
in the target data frame.

E.g. handling of unparameterized types (int to float conversion etc.) is
registered targeting the set of roles that are considered 'numerical', i.e. if a
field is associated with a column that is going to be treated as 'numerical' by
the Engine, the data will be read as float to arrow right away. If the same
field, however, is associated with a column that is treated as string like
('unused_string') the field will be left untouched s.t. the original
representation is preserved.
"""

arrow_schema_field_postprocessor = ArrowSchemaFieldProcessorRegistry()
"""
Registry for arrow schema field postprocessors.

Can be used as a decorator to register a function as a postprocessor for arrow
schema fields.

Postprocessors are applied _after_ the data has been read into arrow but before
the data is sent to the Engine.

Each postprocessor is registered targeting set of roles. Postprocessors
are applied to fields that are associated with any of the registered roles
in the target data frame.

E.g. `postprocess_timestamp_drop_timezone` is registered targeting the set of
roles that are considered as 'numerical' s.t. the timezone information is
dropped only when the timestamp is going to be parsed.
"""


def _create_arrow_metadata(
    df_or_view: Union[DataFrame, View],
) -> Dict[str, str]:
    metadata = {
        "name": df_or_view.name,
        "last_change": df_or_view.last_change,
        "roles": df_or_view.roles.to_dict(),
    }
    metadata_encoded = {"getml": json.dumps(metadata)}
    return metadata_encoded


@contextmanager
def _establish_arrow_stream(
    df_or_view: Union[DataFrame, View],
) -> Tuple[socket.socket, pa.BufferReader, pa.RecordBatchReader]:
    df_or_view.refresh()

    typename = type(df_or_view).__name__

    cmd = {
        "type_": f"{typename}.to_arrow",
        "name_": df_or_view.name if typename == "DataFrame" else "",
    }

    if typename == "View":
        cmd["view_"] = df_or_view._getml_deserialize()

    sock = comm.send_and_get_socket(cmd)
    msg = comm.recv_string(sock)
    if msg != "Success!":
        comm.handle_engine_exception(msg)
    stream = sock.makefile(mode="rb")
    reader = pa.ipc.open_stream(stream)

    with sock, stream, reader:
        yield sock, stream, reader


def _is_numerical_type_arrow(coltype: pa.DataType) -> bool:
    return any(
        [
            pa.types.is_integer(coltype),
            pa.types.is_floating(coltype),
            pa.types.is_decimal(coltype),
        ]
    )


def _is_unsupported_type_arrow(coltype: pa.DataType) -> bool:
    return any(predicate(coltype) for predicate in UNSUPPORTED_TYPE_PREDICATES)


@arrow_schema_field_preprocessor(roles=roles_sets.all_)
def preprocess_timestamp_cast_to_microseconds(field: pa.Field) -> pa.Field:
    """
    Cast timestamps to microseconds to ensure compatibility with the getML engine.
    """
    if pa.types.is_timestamp(field.type):
        return pa.field(
            field.name,
            pa.timestamp(MAX_SUPPORTED_TIMESTAMP_RESOLUTION, tz=field.type.tz),
        )
    return field


@arrow_schema_field_preprocessor(roles={roles.unused_string})
def preprocess_types_cast_to_string(field: pa.Field) -> pa.Field:
    """
    Eagerly cast all fields (except timestamps) that are going to be treated as
    string like to string.
    """
    if pa.types.is_timestamp(field.type):
        return field
    return pa.field(field.name, pa.string())


@arrow_schema_field_preprocessor(roles=roles_sets.numerical)
def preprocess_unparameterized_type(field: pa.Field) -> pa.Field:
    """
    Read unparameterized types as float64 to ensure compatibility with the
    getML engine. Only do so if the column is going to be treated as 'numerical'
    like.
    """
    if field.type in UNPARAMETERIZED_TYPE_CONVERSION_MAPPING:
        return pa.field(
            field.name,
            UNPARAMETERIZED_TYPE_CONVERSION_MAPPING[field.type],
        )
    return field


@arrow_schema_field_preprocessor(roles=roles_sets.numerical)
def preprocess_parameterized_type(field: pa.Field) -> pa.Field:
    """
    Read parameterized types as float64 to ensure compatibility with the
    getML engine. Only do so if the column is going to be treated as 'numerical'
    like.
    """
    if pa.types.is_decimal(field.type):
        warnings.warn(
            DECIMAL_CONVERSION_LOST_PRECISION_WARNING_TEMPLATE.format(
                column_name=field.name
            )
        )
        return pa.field(field.name, pa.float64())
    return field


@arrow_schema_field_postprocessor(roles=roles_sets.numerical)
def postprocess_timestamp_drop_timezone(field: pa.Field) -> pa.Field:
    """
    Drop the timezone information from timestamps if the column is going to be
    parsed (parsing only happens when the target role is numerical).

    Note that columns with the role 'time_stamp' are encoded as float64, s.t.
    the resulting role can be consirederd as 'numerical' like.
    """
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


@arrow_schema_field_postprocessor(roles={roles.unused_string})
def postprocess_timestamp_cast_to_string(field: pa.Field) -> pa.Field:
    """
    Cast timestamps that are going to be treated as string like to string.
    """
    if pa.types.is_timestamp(field.type):
        return pa.field(field.name, pa.string())
    return field


def _process_arrow_schema(
    schema: pa.Schema, roles: Roles, processors: ArrowSchemaFieldProcessorRegistry
) -> pa.Schema:
    processed_fields = []
    for field in schema:
        if field.name not in roles.columns:
            continue
        role = roles.column(field.name)
        field_processed = field.with_metadata({})
        for processor in processors.retrieve(role):
            field_processed = processor(field_processed)
        processed_fields.append(field_processed)
    return pa.schema(processed_fields)


def postprocess_arrow_schema(schema: pa.Schema, roles: Roles) -> pa.Schema:
    return _process_arrow_schema(schema, roles, arrow_schema_field_postprocessor)


def preprocess_arrow_schema(schema: pa.Schema, roles: Roles) -> pa.Schema:
    return _process_arrow_schema(schema, roles, arrow_schema_field_preprocessor)


def sniff_schema(schema: pa.Schema, colnames: Iterable[str] = ()) -> Roles:
    arrow_metadata = schema.metadata or {}
    if metadata := arrow_metadata.get(b"getml"):
        metadata_unmarshaled = json.loads(metadata)
        roles = metadata_unmarshaled["roles"]
        return Roles.from_dict(roles)

    if metadata := arrow_metadata.get(b"PANDAS_ATTRS", {}).get(b"getml"):
        metadata_unmarshaled = json.loads(metadata)
        roles = metadata_unmarshaled["roles"]
        return Roles.from_dict(roles)

    roles: Dict[str, List[str]] = {}
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
    metadata = _create_arrow_metadata(df_or_view)
    with to_arrow_stream(df_or_view) as reader:
        return reader.read_all().replace_schema_metadata(metadata)

@contextmanager
def to_arrow_stream(
    df_or_view: Union[DataFrame, View],
) -> Iterator[pa.RecordBatchReader]:
    with _establish_arrow_stream(df_or_view) as (sock, stream, reader):
        yield reader
        
def to_arrow_batches(
    batch_or_batches: Union[pa.RecordBatch, pa.Table, Iterable[pa.RecordBatch]],
) -> Tuple[pa.Schema, Iterator[pa.RecordBatch]]:
    if isinstance(batch := batch_or_batches, pa.RecordBatch):
        batch = cast(pa.RecordBatch, batch)
        return batch.schema, iter([batch])
    elif isinstance(batches := batch_or_batches, Iterable):
        if all(isinstance(batch, pa.RecordBatch) for batch in batches):
            first_batch = cast(pa.RecordBatch, next(iter(batches)))
            return first_batch.schema, iter(batches)
        else:
            raise TypeError(
                "If 'data' is an iterable, all elements must be of type pa.RecordBatch."
            )
    elif isinstance(table := batch_or_batches, pa.Table):
        return table.schema, iter(table.to_batches(max_chunksize=DEFAULT_BATCH_SIZE))  # type: ignore
    else:
        raise TypeError(
            "'data' must be a pa.RecordBatch, pa.Table, or an iterable of pa.RecordBatch instances."
        )


@arrow_cast_exception_handler(source_type=pa.int64(), target_type=pa.float64())
def handle_int64_to_float64_exc(
    exc: ArrowInvalid, source_array: pa.Array, target_field: pa.Field
):
    if re.match(r"Integer value \d+ not in range", str(exc)):
        raise OverflowError(
            INT64_EXCEEDS_FLOAT64_BOUNDS_ERROR_TEMPLATE.format(
                column_name=target_field.name
            )
        ) from exc
    raise exc


@arrow_cast_exception_handler(source_type=pa.binary(), target_type=pa.string())
def handle_binary_to_string_exc(
    exc: ArrowInvalid, source_array: pa.Array, target_field: pa.Field
):
    if re.match(r"Invalid UTF8 payload", str(exc)):
        raise ValueError(
            f"Binary column '{target_field.name}' contains invalid UTF-8 characters.\n"
            "First 5 rows of the column:\n"
            f"{source_array.slice(0, 5)}"
        )


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
            cast_options = pa_compute.CastOptions(
                target_type=target_field.type,
                allow_time_truncate=True,
            )
            try:
                target_arrays.append(source_array.cast(options=cast_options))
            except ArrowInvalid as e:
                handler = arrow_cast_exception_handler.retrieve(
                    source_type=source_array.type, target_type=target_field.type
                )
                handler(e, source_array, target_field)
        else:
            target_arrays.append(source_array)
    return pa.RecordBatch.from_arrays(target_arrays, schema=target_schema)


def read_arrow_batches(
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    df: DataFrame,
    append: bool = False,
) -> None:
    cmd = {
        "type_": "DataFrame.from_arrow",
        "name_": df.name,
        "append_": append,
        "categorical_": df._categorical_names,
        "join_keys_": df._join_key_names,
        "numerical_": df._numerical_names,
        "targets_": df._target_names,
        "text_": df._text_names,
        "time_stamps_": df._time_stamp_names,
        "unused_floats_": df._unused_float_names,
        "unused_strings_": df._unused_string_names,
    }

    processed_schema = postprocess_arrow_schema(schema, df.roles)

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
