from dataclasses import dataclass

import pytest

from getml.data.helpers import _infer_arange_args_from_slice as infer


@dataclass
class FakeDFHasLen:
    length: int

    def __len__(self) -> int:
        return self.length


@pytest.fixture
def fake_df_has_len_10():
    return FakeDFHasLen(10)


testdata_infer_arange_args_from_slice = [
    (slice(None, None, None), (0, 10, 1)),
    (slice(0, None, None), (0, 10, 1)),
    (slice(0, 10, None), (0, 10, 1)),
    (slice(0, 10, 1), (0, 10, 1)),
    (slice(0, 10, 2), (0, 10, 2)),
    (slice(0, 10, -1), (0, 0, 1)),
    (slice(0, 10, -2), (0, 0, 1)),
    (slice(10, 0, -1), (10, 0, -1)),
    (slice(10, 0, -2), (10, 0, -2)),
    (slice(10, 0, 1), (0, 0, 1)),
    (slice(-10, None, None), (0, 10, 1)),
    (slice(-10, 10, None), (0, 10, 1)),
    (slice(-10, 10, 1), (0, 10, 1)),
    (slice(-10, 10, 2), (0, 10, 2)),
    (slice(-10, 10, -1), (0, 0, 1)),
    (slice(-10, 10, -2), (0, 0, 1)),
    (slice(-6, -4, 1), (4, 6, 1)),
    (slice(-6, -4, -1), (0, 0, 1)),
    (slice(-6, -4, 2), (4, 6, 2)),
    (slice(-4, -6, 1), (0, 0, 1)),
    (slice(None, None, 1), (0, 10, 1)),
    (slice(None, None, -1), (9, -1, -1)),
    (slice(5, None, 1), (5, 10, 1)),
    (slice(5, None, -1), (5, -1, -1)),
    (slice(None, 5, 1), (0, 5, 1)),
    (slice(None, 5, -1), (9, 5, -1)),
    # we don't care about upper bounds on the python side and let the Engine
    # handle it lazily
    (slice(0, 100, 1), (0, 100, 1)),
    (slice(0, 100, -1), (0, 0, 1)),
    (slice(0, 100, 2), (0, 100, 2)),
    # as we have a natural zero, we can immediately handle lower bounds, though
    (slice(100, 0, -1), (100, 0, -1)),
    (slice(100, 0, 1), (0, 0, 1)),
    (slice(-100, 0, 1), (0, 0, 1)),
]


@pytest.mark.parametrize("given_slice, expected", testdata_infer_arange_args_from_slice)
def test_infer_arange_args_from_slice(fake_df_has_len_10, given_slice, expected):
    assert infer(given_slice, fake_df_has_len_10) == expected
