from dataclasses import dataclass

import pytest

import getml
from getml.data.helpers import _infer_arange_args_from_slice as infer


@dataclass
class FakeDFHasLen:
    length: int

    def __len__(self) -> int:
        return self.length


@pytest.fixture
def fake_df_has_len_10():
    return FakeDFHasLen(10)


@pytest.mark.parametrize("df_or_view", ["df", "view"], indirect=True)
def test_boolean_subsetting(df_or_view):
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
def test_scalar_subsetting(df_or_view):
    assert len(df_or_view[0]) == 1
    assert len(df_or_view[len(df_or_view) - 1]) == 1
    assert len(df_or_view[-1]) == 1
    assert len(df_or_view[-len(df_or_view)]) == 1
    assert df_or_view[-len(df_or_view)].to_arrow() == df_or_view[0].to_arrow()
    assert df_or_view[len(df_or_view) - 1].to_arrow() == df_or_view[-1].to_arrow()


def test_infer_arange_args_from_slice(fake_df_has_len_10):
    fk = fake_df_has_len_10
    assert infer(slice(None, None, None), fk) == (0, 10, 1)
    assert infer(slice(0, None, None), fk) == (0, 10, 1)
    assert infer(slice(0, 10, None), fk) == (0, 10, 1)
    assert infer(slice(0, 10, 1), fk) == (0, 10, 1)
    assert infer(slice(0, 10, 2), fk) == (0, 10, 2)
    assert infer(slice(0, 10, -1), fk) == (10, 0, -1)
    assert infer(slice(0, 10, -2), fk) == (10, 0, -2)
    assert infer(slice(10, 0, -1), fk) == (0, 0, -1)
    assert infer(slice(10, 0, -2), fk) == (0, 0, -2)
    assert infer(slice(10, 0, 1), fk) == (0, 0, 1)
    assert infer(slice(-10, None, None), fk) == (0, 10, 1)
    assert infer(slice(-10, 10, None), fk) == (0, 10, 1)
    assert infer(slice(-10, 10, 1), fk) == (0, 10, 1)
    assert infer(slice(-10, 10, 2), fk) == (0, 10, 2)
    assert infer(slice(-10, 10, -1), fk) == (10, 0, -1)
    assert infer(slice(-10, 10, -2), fk) == (10, 0, -2)
    assert infer(slice(-6, -4, 1), fk) == (4, 6, 1)
    assert infer(slice(-6, -4, -1), fk) == (6, 4, -1)
    assert infer(slice(-6, -4, 2), fk) == (4, 6, 2)
    assert infer(slice(-4, -6, 1), fk) == (0, 0, 1)
    # we don't care about upper bounds on the python side and let the engine
    # handle it lazily
    assert infer(slice(0, 100, 1), fk) == (0, 100, 1)
    assert infer(slice(0, 100, -1), fk) == (100, 0, -1)
    assert infer(slice(0, 100, 2), fk) == (0, 100, 2)
    # as we have a natural zero, we can immediately handle lower bounds, though
    assert infer(slice(100, 0, -1), fk) == (0, 0, -1)
    assert infer(slice(100, 0, 1), fk) == (0, 0, 1)
    assert infer(slice(-100, 0, 1), fk) == (0, 0, 1)


@pytest.mark.parametrize("df_or_view", ["df", "view"], indirect=True)
def test_slicing(df_or_view):
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
