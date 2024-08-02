from dataclasses import dataclass

import pytest
import getml


@pytest.mark.parametrize("df_or_view", ["df", "view"], indirect=True)
def test_boolean_subsetting(getml_project, df_or_view):
    unique_values = df_or_view.column_01.unique()
    assert len(df_or_view[df_or_view.column_01 > df_or_view.column_01.max()]) == 0
    assert len(df_or_view[df_or_view.column_01 < df_or_view.column_01.min()]) == 0
    assert len(df_or_view[df_or_view.column_01 == unique_values[0]]) == 1
    assert (
        df_or_view[df_or_view.column_01 == unique_values[0]].column_01[0]
        == unique_values[0]
    )
    assert (
        len(df_or_view[df_or_view.column_01 != unique_values[0]])
        == len(unique_values) - 1
    )
    assert df_or_view[df_or_view.column_01 != unique_values[0]].nrows() == "unknown"


@pytest.mark.parametrize("df_or_view", ["df", "view"], indirect=True)
def test_scalar_subsetting(getml_project, df_or_view):
    assert len(df_or_view[0]) == 1
    assert len(df_or_view[len(df_or_view) - 1]) == 1
    assert len(df_or_view[-1]) == 1
    assert len(df_or_view[-len(df_or_view)]) == 1
    assert df_or_view[-len(df_or_view)].to_arrow() == df_or_view[0].to_arrow()
    assert df_or_view[len(df_or_view) - 1].to_arrow() == df_or_view[-1].to_arrow()


@pytest.mark.parametrize("df_or_view", ["df", "view"], indirect=True)
def test_slicing(getml_project, df_or_view):
    assert len(df_or_view[:]) == len(df_or_view)
    assert len(df_or_view[: len(df_or_view)]) == len(df_or_view)
    assert len(df_or_view[len(df_or_view) :]) == 0
    assert len(df_or_view[-len(df_or_view) :]) == len(df_or_view)
    assert len(df_or_view[: -len(df_or_view)]) == 0
    assert len(df_or_view[-len(df_or_view) : len(df_or_view)]) == len(df_or_view)
    assert len(df_or_view[-6:-4]) == 2
    assert len(df_or_view[-4:-6]) == 0
    assert df_or_view[:].to_arrow() == df_or_view.to_arrow()
    assert df_or_view[4:6].to_arrow() == df_or_view.to_arrow()[4:6]
    assert df_or_view[-4:-6].to_arrow() == df_or_view.to_arrow()[-4:-6]
    assert df_or_view[-6:-4].to_arrow() == df_or_view.to_arrow()[-6:-4]
    assert df_or_view[-6:-4:-1].to_arrow() == df_or_view.to_arrow()[-6:-4:-1]


@pytest.mark.parametrize("df_or_view", ["df", "view"], indirect=True)
def test_by_column(getml_project, df_or_view):
    ix = getml.data.columns.arange(0, 10, 1)
    reverse_ix = getml.data.columns.arange(9, -1, -1)

    assert len(ix) == 10
    assert len(reverse_ix) == 10

    assert len(ix[ix]) == 10
    assert len(reverse_ix[ix]) == 10

    assert len(ix[reverse_ix]) == 10
    assert len(reverse_ix[reverse_ix]) == 10

    assert len(df_or_view[ix]) == 10
    assert len(df_or_view[reverse_ix]) == 10
