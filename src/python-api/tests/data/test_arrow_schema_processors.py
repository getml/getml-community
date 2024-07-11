import pyarrow as pa
import pytest

from getml.data._io.arrow import (
    DECIMAL_CONVERSION_LOST_PRECISION_WARNING_TEMPLATE,
    TIMEZONE_UTC_DROPPED_WARNING_TEMPLATE,
    process_parameterized_type,
    process_time_stamp,
    process_unparameterized_type,
)


def test_process_timestamp(timestamp_batch):
    with pytest.warns(UserWarning) as warnings:
        processed_fields = [
            process_time_stamp(field) for field in timestamp_batch.schema
        ]
    assert str(warnings[0].message) == TIMEZONE_UTC_DROPPED_WARNING_TEMPLATE.format(
        column_name=timestamp_batch.schema[0].name,
    )
    assert all(pa.types.is_timestamp(field.type) for field in processed_fields)


def test_process_int(int_batch):
    processed_fields = [
        process_unparameterized_type(field) for field in int_batch.schema
    ]
    assert all(pa.types.is_float64(field.type) for field in processed_fields)


def test_process_decimal(decimal_batch):
    with pytest.warns(UserWarning) as warnings:
        processed_fields = [
            process_parameterized_type(field) for field in decimal_batch.schema
        ]
    assert str(
        warnings[0].message
    ) == DECIMAL_CONVERSION_LOST_PRECISION_WARNING_TEMPLATE.format(
        column_name=decimal_batch.schema[0].name,
        precision=decimal_batch.schema[0].type.precision,
    )
    assert all(pa.types.is_float64(field.type) for field in processed_fields)
