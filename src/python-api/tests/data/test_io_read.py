import pytest

import getml


def test_read_parquet(parquet_file, engine):
    df = getml.DataFrame.from_parquet(parquet_file, name="df")
    assert df.shape == (3, 2)
    assert df.columns == ["a", "b"]
    assert df["a"].to_numpy().tolist() == [1, 2, 3]  # type: ignore
    assert df["b"].to_numpy().tolist() == [4, 5, 6]  # type: ignore


def test_read_csv(csv_file, engine):
    df = getml.DataFrame.from_csv(csv_file, name="df")
    assert df.shape == (4, 4)
    assert sorted(df.columns) == sorted(["name", "column_01", "join_key", "time_stamp"])
    assert df["column_01"].to_numpy().tolist() == list(range(4))  # type: ignore
    assert df["join_key"].to_numpy().tolist() == list(range(22, 26))  # type: ignore
    assert df["time_stamp"].to_numpy().tolist() == [  # type: ignore
        "2019-01-01",
        "2019-01-02",
        "2019-01-03",
        "2019-01-04",
    ]


def test_read_csv_num_lines_sniffed_deprecation_warning(csv_file, engine):
    with pytest.warns(DeprecationWarning):
        getml.DataFrame.from_csv(csv_file, num_lines_sniffed=10, name="df")


def test_read_csv_custom_ts(csv_file_custom_ts, engine):
    df = getml.DataFrame(name="Testiana", roles={"time_stamp": ["time_stamp"]})
    df.read_csv(csv_file_custom_ts, time_formats=["date: %Y-%m-%d; time: %H:%M:%S"])
    assert df.roles.time_stamp == ("time_stamp",)
