"""
Utilties for rolling data frames.
"""
import warnings
from typing import Generic, Iterator, NamedTuple, Optional, TypeVar

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import as_strided
from typing_extensions import TypeGuard

IndexT = TypeVar("IndexT", pd.Timestamp, int)
RangeT = TypeVar("RangeT", pd.Timedelta, int)


def _is_int(val: object) -> TypeGuard[int]:
    return isinstance(val, int)


def _is_timedelta(val: object) -> TypeGuard[pd.Timedelta]:
    return isinstance(val, (pd.Timedelta))


class _ChunkMaker(NamedTuple):
    """
    Helpers class to create chunks of data frames.
    """

    data_frame: pd.DataFrame
    horizon: RangeT
    memory: RangeT
    min_chunksize: int

    def make_chunk(self, now: IndexT) -> pd.DataFrame:
        """
        Generates a chunk of the data frame that
        contains all rows within horizon and memory.

        Used by roll_data_frame.
        """

        index_0 = self.data_frame.index[0]

        begin = now - self.horizon - self.memory
        end = now - self.horizon

        if isinstance(self.data_frame.index, pd.RangeIndex):
            begin = max(index_0, begin)
            end = max(index_0, end)

        # we want to exclude the lower bound we use a hard (iloc) slice to make this
        # independent from the frequency, so we have to ensure that the chunk actually
        # contains the lower bound
        if _is_timedelta(self.memory):
            if not begin < index_0:
                return self.data_frame[begin:end][1:]

        return self.data_frame[begin:end]


# ------------------------------------------------------------


def _roll_data_frame(
    data_frame: pd.DataFrame,
    time_stamp: Optional[str] = None,
    horizon: RangeT = 1,
    memory: RangeT = 10,
    min_chunksize: int = 1,
) -> Iterator[pd.DataFrame]:
    """
    Returns an iterator that yields pd.DataFrame chunks
    to be aggregated.
    """
    if len(data_frame) < min_chunksize + 1:
        raise ValueError

    if time_stamp:
        data_frame = data_frame.set_index(time_stamp)

    chunk_maker = _ChunkMaker(data_frame, horizon, memory, min_chunksize)

    # when working with slices, pandas' .loc includes its upper bound while .iloc does
    # not. we need to correct for that.
    if isinstance(data_frame.index, pd.DatetimeIndex):
        # the following indirection is nessecary because we cannot correct the value of
        # memory inside make_chunk (to exclude one of the bounds) without knowing the
        # time stamp's frequency. we shift the index by 1 to exclude the upper bound
        # (at the cost of including one observation beyond the lower bound). the shift
        # is necessery to allow the index run though its end. within make_chunk, we will
        # chop off the first observation if appropriate
        offset = data_frame.index[min_chunksize - 1] + horizon
        return (chunk_maker.make_chunk(now) for now in data_frame[offset:].index)

    else:
        return (
            chunk_maker.make_chunk(i)
            for i in range(min_chunksize + horizon, len(data_frame) + 1)
        )


# ------------------------------------------------------------


def _roll_strided(arr: np.ndarray, horizon: int, memory: int) -> np.ndarray:
    stride = arr.strides[0]
    arr_strided = as_strided(
        arr,
        shape=(len(arr) - memory - horizon, memory + horizon),
        strides=(stride, stride),
        writeable=False,
    )
    if horizon > 0:
        return arr_strided[:, horizon:]
    return arr_strided
