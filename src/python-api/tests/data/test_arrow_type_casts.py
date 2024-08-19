import pyarrow as pa
import pytest

import getml
from getml.data._io.arrow import (
    INT64_EXCEEDS_FLOAT64_BOUNDS_ERROR_TEMPLATE,
    TIMEZONE_NON_UTC_ERROR_TEMPLATE,
    TIMEZONE_UTC_DROPPED_WARNING_TEMPLATE,
)


def test_from_arrow_utc_timestamp(getml_project, timestamp_with_utc_tz_batch):
    with pytest.warns(UserWarning) as warnings:
        df = getml.data.DataFrame.from_arrow(
            timestamp_with_utc_tz_batch,
            name="tz_utc",
            roles={"time_stamp": ["utc_time"]},
        )
    assert str(warnings[0].message) == TIMEZONE_UTC_DROPPED_WARNING_TEMPLATE.format(
        column_name=timestamp_with_utc_tz_batch.schema[0].name,
    )
    assert "utc_time" in df.roles.time_stamp
    assert (
        df.to_arrow()
        .to_batches()[0][0]
        .equals(timestamp_with_utc_tz_batch[0].cast(pa.timestamp("ns", tz=None)))
    )
    assert (
        df.to_arrow()["utc_time"].type.unit
        == timestamp_with_utc_tz_batch.schema[0].type.unit
    )
    assert df.to_arrow()["utc_time"].type == pa.timestamp("ns", tz=None)
    assert df.to_arrow()["utc_time"].type.tz is None


def test_from_arrow_non_utc_timestamp(getml_project, timestamp_with_non_utc_tz_batch):
    with pytest.raises(TypeError) as exc_info:
        getml.data.DataFrame.from_arrow(
            timestamp_with_non_utc_tz_batch,
            name="tz_non_utc",
            roles={"time_stamp": ["non_utc_time"]},
        )
    assert str(exc_info.value) == TIMEZONE_NON_UTC_ERROR_TEMPLATE.format(
        column_name=timestamp_with_non_utc_tz_batch.schema[0].name,
        timezone=timestamp_with_non_utc_tz_batch.schema[0].type.tz,
    )


def test_from_arrow_no_tz_timestamp(getml_project, timestamp_without_tz_batch):
    df = getml.data.DataFrame.from_arrow(
        timestamp_without_tz_batch, name="tz_none", roles={"time_stamp": ["no_tz_time"]}
    )
    assert "no_tz_time" in df.roles.time_stamp
    assert df.to_arrow().to_batches()[0].equals(timestamp_without_tz_batch)
    assert (
        df.to_arrow()["no_tz_time"].type.unit
        == timestamp_without_tz_batch.schema[0].type.unit
    )
    assert df.to_arrow()["no_tz_time"].type == pa.timestamp("ns", tz=None)
    assert df.to_arrow()["no_tz_time"].type.tz is None


def test_from_arrow_int(getml_project, int_batch):
    with pytest.raises(OverflowError) as exc_info:
        getml.data.DataFrame.from_arrow(int_batch, name="int")
    assert str(exc_info.value) == INT64_EXCEEDS_FLOAT64_BOUNDS_ERROR_TEMPLATE.format(
        column_name="int64_exceeds_float64_upper_bound",
    )
