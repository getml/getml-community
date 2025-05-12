import pytest


@pytest.mark.parametrize("df", ["df1", "df2", "df3"], indirect=True)
def test_read_arrow_stream(getml_project, df):
    with df.to_arrow_stream() as stream:
        assert stream is not None
        for batch in stream:
            assert batch is not None
            assert len(batch) > 0
            assert len(batch.column_names) > 0
            assert set(batch.column_names) == set(df.columns)
