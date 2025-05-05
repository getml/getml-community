import json

import pyarrow.parquet as pq
import pytest

import getml
from tests.conftest import workdir


def test_write_csv(getml_project, tmpdir):
    with workdir(tmpdir):
        df = getml.DataFrame.from_dict(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
            },
            name="df",
        )
        df.to_csv("test.csv")
        df2 = getml.DataFrame.from_csv("test.csv", name="df2")
        assert df2.shape == (3, 2)
        assert df2.columns == ["a", "b"]
        assert df2["a"].to_numpy().tolist() == [1, 2, 3]  # type: ignore


def test_write_parquet(getml_project, tmpdir):
    with workdir(tmpdir):
        df = getml.DataFrame.from_dict(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
            },
            name="df",
        )
        df.to_parquet("test.parquet")
        df2 = getml.DataFrame.from_parquet("test.parquet", name="df2")
        assert df2.shape == (3, 2)
        assert df2.columns == ["a", "b"]
        assert df2["a"].to_numpy().tolist() == [1, 2, 3]  # type: ignore


@pytest.mark.parametrize("df", ["df1", "df2", "df3"], indirect=True)
def test_to_parquet_metadata(getml_project, df, tmpdir):
    with workdir(tmpdir):
        df.to_parquet("test.parquet")
        schema = pq.read_schema("test.parquet")
        metadata = schema.metadata[b"getml"]
        metadata_unmarshaled = json.loads(metadata)
        assert metadata_unmarshaled["name"] == df.name
        assert metadata_unmarshaled["last_change"] == df.last_change
        assert metadata_unmarshaled["roles"] == df.roles.to_dict()
