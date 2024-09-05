import pyarrow as pa
import pyarrow.csv as pa_csv
import pytest

from getml.data._io.arrow import (
    DECIMAL_CONVERSION_LOST_PRECISION_WARNING_TEMPLATE,
    MAX_SUPPORTED_TIMESTAMP_RESOLUTION,
    TIMEZONE_UTC_DROPPED_WARNING_TEMPLATE,
    postprocess_arrow_schema,
    postprocess_timestamp_drop_timezone,
    preprocess_arrow_schema,
    preprocess_parameterized_type,
    preprocess_timestamp_cast_to_microseconds,
    preprocess_unparameterized_type,
)
from getml.data.roles.container import Roles


@pytest.fixture
def schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("time_stamp", pa.timestamp("ns", tz="UTC")),
            pa.field("join_key", pa.int32()),
            pa.field("target", pa.int32()),
            pa.field("column_01", pa.float64()),
        ]
    )


def _get_inferrence_blob_block_size(csv_blob: str, n_lines_inferred: int = 1) -> int:
    lines = csv_blob.splitlines()
    inferrence_blob = "\n".join(lines[: 1 + n_lines_inferred]).encode()
    return len(inferrence_blob) + 1  # 1 byte for the trailing newline character


def test_process_timestamp(timestamp_batch):
    preprocessed_fields = [
        preprocess_parameterized_type(field) for field in timestamp_batch.schema
    ]
    assert all(pa.types.is_timestamp(field.type) for field in preprocessed_fields)
    preprocessed_timestamps_microseconds = [
        preprocess_timestamp_cast_to_microseconds(field)
        for field in timestamp_batch.schema
    ]
    assert all(
        pa.types.is_timestamp(field.type)
        for field in preprocessed_timestamps_microseconds
    )
    assert all(
        field.type.unit == MAX_SUPPORTED_TIMESTAMP_RESOLUTION
        for field in preprocessed_timestamps_microseconds
    )
    with pytest.warns(UserWarning) as warnings:
        postprocessed_fields_dropped_tz = [
            postprocess_timestamp_drop_timezone(field)
            for field in timestamp_batch.schema
        ]
    assert str(warnings[0].message) == TIMEZONE_UTC_DROPPED_WARNING_TEMPLATE.format(
        column_name=timestamp_batch.schema[0].name,
    )
    assert all(
        pa.types.is_timestamp(field.type) for field in postprocessed_fields_dropped_tz
    )

    ts_roles = Roles(time_stamp=["utc_time", "no_tz_time"])

    preprocessed_schema = preprocess_arrow_schema(timestamp_batch.schema, ts_roles)

    assert all(pa.types.is_timestamp(field.type) for field in preprocessed_schema)
    assert all(
        field.type.unit == MAX_SUPPORTED_TIMESTAMP_RESOLUTION
        for field in preprocessed_schema
    )

    postprocessed_schema_target_timestamp = postprocess_arrow_schema(
        preprocessed_schema, ts_roles
    )
    assert all(
        pa.types.is_timestamp(field.type)
        for field in postprocessed_schema_target_timestamp
    )
    assert all(
        field.type.unit == MAX_SUPPORTED_TIMESTAMP_RESOLUTION
        for field in postprocessed_schema_target_timestamp
    )
    assert all(field.type.tz is None for field in postprocessed_schema_target_timestamp)

    string_roles = Roles(unused_string=["utc_time", "no_tz_time"])
    postprocessed_schema_target_string = postprocess_arrow_schema(
        preprocessed_schema, string_roles
    )
    assert all(
        pa.types.is_string(field.type) for field in postprocessed_schema_target_string
    )


def test_process_int(int_batch):
    processed_fields = [
        preprocess_unparameterized_type(field) for field in int_batch.schema
    ]
    assert all(pa.types.is_float64(field.type) for field in processed_fields)


def test_process_decimal(decimal_batch):
    with pytest.warns(UserWarning) as warnings:
        processed_fields = [
            preprocess_parameterized_type(field) for field in decimal_batch.schema
        ]
    assert str(
        warnings[0].message
    ) == DECIMAL_CONVERSION_LOST_PRECISION_WARNING_TEMPLATE.format(
        column_name=decimal_batch.schema[0].name,
        precision=decimal_batch.schema[0].type.precision,
    )
    assert all(pa.types.is_float64(field.type) for field in processed_fields)


def test_schema_preprocessing(schema):
    specific_roles = Roles(
        time_stamp=["time_stamp"],
        join_key=["join_key"],
        target=["target"],
        numerical=["column_01"],
    )
    preprocessed_schema = preprocess_arrow_schema(schema, specific_roles)
    # time stamp: target role is timestamp, timezone is dropped, resolution is
    # cast to microseconds
    assert preprocessed_schema[0].type == pa.timestamp(
        MAX_SUPPORTED_TIMESTAMP_RESOLUTION, preprocessed_schema[0].type.tz
    )
    # join_key: target role is categorical like, int32 is left untouched
    assert preprocessed_schema[1].type == pa.int32()
    # target: target role is neither numerical like not categorical like, type
    # is untouched
    assert preprocessed_schema[2].type == pa.float64()
    # target role is numerical like, type is already compliant with getml
    assert preprocessed_schema[3].type == pa.float64()


def test_schema_postprocessing(schema):
    specific_roles = Roles(
        time_stamp=["time_stamp"],
        join_key=["join_key"],
        target=["target"],
        numerical=["column_01"],
    )
    with pytest.warns(UserWarning) as warnings:
        postprocessed_schema = postprocess_arrow_schema(schema, specific_roles)
    assert str(warnings[0].message) == TIMEZONE_UTC_DROPPED_WARNING_TEMPLATE.format(
        column_name="time_stamp",
    )
    # time stamp: target role is timestamp, timezone is dropped, resolution is
    # untouched at postprocessing
    assert postprocessed_schema[0].type.tz is None
    assert postprocessed_schema[0].type.unit != MAX_SUPPORTED_TIMESTAMP_RESOLUTION
    assert postprocessed_schema[0].type.unit == "ns"
    assert [field.type for field in list(postprocessed_schema)[1:]] == [
        field.type for field in list(schema)[1:]
    ]

    string_roles = Roles(
        unused_string=["time_stamp", "join_key", "target", "column_01"]
    )
    postprocessed_schema = postprocess_arrow_schema(schema, string_roles)
    # target role is unused_string: all fields are cast to string
    assert all(pa.types.is_string(field.type) for field in postprocessed_schema)


def test_arrow_inference_csv_changing_type(csv_file_with_changing_type_in_row_2):
    source = csv_file_with_changing_type_in_row_2
    with open(source, "r") as f:
        inferrence_blob_block_size = _get_inferrence_blob_block_size(
            f.read(), n_lines_inferred=1
        )
    ro = pa_csv.ReadOptions(block_size=inferrence_blob_block_size)

    batches = pa_csv.open_csv(source, read_options=ro)

    next(batches)

    # The second batch has a different type for the column "targets"
    # s.t. it is not compliant with the schema of the first batch
    with pytest.raises(pa.ArrowInvalid):
        next(batches)

    # when reading the csv in full, pyarrow inferrs the correct schema
    table = pa_csv.read_csv(source, read_options=ro)

    assert table.schema != batches.schema
