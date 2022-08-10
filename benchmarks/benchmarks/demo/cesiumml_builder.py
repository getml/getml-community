"""
Wrapper around CesiumML
"""

import datetime
import time
from typing import Generator, NamedTuple, List
import warnings

from cesium import featurize  # type: ignore
from cesium.features import feature_categories  # type: ignore
import numpy as np
import pandas as pd  # type: ignore
from pandas.api.types import is_numeric_dtype  # type: ignore
from scipy.stats import pearsonr  # type: ignore

from .add_original_columns import _add_original_columns
from .print_time_taken import _print_time_taken
from .remove_target_column import _remove_target_column

# ------------------------------------------------------------------


class _ChunkMaker(NamedTuple):
    """
    Helpers class to create chunks of data frames.
    """

    data_frame: pd.DataFrame
    id_col: pd.Series
    time_col: str
    horizon: pd.Timedelta
    memory: pd.Timedelta

    def make_chunk(self, current_id: str, now: pd.Timedelta) -> pd.DataFrame:
        """
        Generates a chunk of the data frame that
        contains all rows within horizon and memory.

        Used by roll_data_frame.
        """
        begin = now - self.horizon - self.memory
        end = now - self.horizon
        return self.data_frame[
            (self.id_col == current_id)
            & (self.time_col > begin)
            & (self.time_col <= end)
        ]


# ------------------------------------------------------------------


def _roll_data_frame(
    data_frame: pd.DataFrame,
    column_id: str,
    time_stamp: str,
    horizon: pd.Timedelta,
    memory: pd.Timedelta,
) -> Generator[pd.DataFrame, None, None]:
    """
    Returns a generator that contains pd.DataFrame chunks
    to be aggregated.
    """
    id_col = data_frame[column_id]
    time_col = pd.to_datetime(data_frame[time_stamp])
    chunk_maker = _ChunkMaker(data_frame, id_col, time_col, horizon, memory)
    return (
        chunk_maker.make_chunk(row[column_id], pd.to_datetime(row[time_stamp]))
        for _, row in data_frame.iterrows()
    )


# ------------------------------------------------------------------


def _aggregate_chunk(
    chunk: pd.DataFrame, column_id: str, time_stamp: str, aggregations: List[str]
) -> pd.DataFrame:
    orig_colnames = [col for col in chunk.columns if col not in (time_stamp, column_id)]
    times = [np.asarray(chunk[time_stamp])] * len(orig_colnames)
    values = [np.asarray(chunk[col]) for col in orig_colnames]
    if chunk.shape[0] > 1:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fset_cesium = featurize.featurize_time_series(
                times=times, values=values, errors=None, features_to_use=aggregations
            )
            data = fset_cesium.values.flatten()
    else:
        data = [np.nan] * len(orig_colnames) * len(aggregations)
    colnames = [
        agg + "(" + cname + ")" for cname in orig_colnames for agg in aggregations
    ]
    return pd.DataFrame({key: [value] for (key, value) in zip(colnames, data)})


# ------------------------------------------------------------------


def _aggregate_chunks(
    rolled: Generator[pd.DataFrame, None, None],
    column_id: str,
    time_stamp: str,
    aggregations: List[str],
) -> pd.DataFrame:
    aggregated_chunks: List[pd.DataFrame] = [
        _aggregate_chunk(chunk, column_id, time_stamp, aggregations) for chunk in rolled
    ]
    return pd.concat(aggregated_chunks, ignore_index=True).reset_index()


# ------------------------------------------------------------------


class CesiumMLBuilder:
    """
    Scikit-learn-style feature builder based on CesiumML.

    Args:
        aggregations: The aggregations to use.

        num_features: The (maximum) number of features to build.

        horizon: The prediction horizon to use.

        memory: How much back in time you want to go until the
                feature builder starts "forgetting" data.

        column_id: The name of the column containing the ids.

        time_stamp: The name of the column containing the time stamps.

        target: The name of the target column.
    """

    cadence_aggs: List[str] = feature_categories["Cadence/Error"]
    general_aggs: List[str] = feature_categories["General"]
    periodic_aggs: List[str] = feature_categories["Lomb-Scargle (Periodic)"]
    all_aggs = cadence_aggs + general_aggs + periodic_aggs

    def __init__(
        self,
        num_features: int,
        horizon: pd.Timedelta,
        memory: pd.Timedelta,
        column_id: str,
        time_stamp: str,
        target: str,
        aggregations: List[str] = general_aggs,
        allow_lagged_targets: bool = False,
        n_jobs=0,
        min_chunksize=0,
    ):
        self.aggregations = aggregations
        self.num_features = num_features
        self.horizon = horizon
        self.memory = memory
        self.column_id = column_id
        self.time_stamp = time_stamp
        self.target = target
        self.allow_lagged_targets = allow_lagged_targets

        self._runtime = None
        self.fitted = False
        self.max_depth = 2

        self.num_features_generated = 0
        self.selected_features: List[int] = []

    def _extract_features(self, data_frame):
        data_frame = data_frame.reset_index()
        del data_frame["index"]
        rolled = _roll_data_frame(
            data_frame, self.column_id, self.time_stamp, self.horizon, self.memory
        )
        df_extracted = _aggregate_chunks(
            rolled, self.column_id, self.time_stamp, self.aggregations
        )
        for col in df_extracted:
            if is_numeric_dtype(df_extracted[col]):
                df_extracted[col][df_extracted[col].isna()] = 0
        return df_extracted

    def _select_features(self, data_frame, target):
        colnames = np.asarray(data_frame.columns)
        print("Selecting the best out of " + str(len(colnames)) + " features...")
        colnames = np.asarray(
            [
                col
                for col in colnames
                if is_numeric_dtype(data_frame[col])
                and np.var(np.asarray(data_frame[col])) > 0.0
            ]
        )
        correlations = np.asarray(
            [np.abs(pearsonr(target, data_frame[col]))[0] for col in colnames]
        )
        correlations[np.isnan(correlations) | np.isinf(correlations)] = 0.0

        self.selected_features = colnames[np.argsort(correlations)][::-1][
            : self.num_features
        ]
        self.num_features_generated = data_frame.shape[1]
        return data_frame[self.selected_features]

    def fit(self, data_frame):
        """
        Fits the DFS on the data frame and returns
        the features for the training set.
        """
        print("CesiumML: Trying features...")
        begin = time.time()
        target = np.asarray(data_frame[self.target])
        df_for_extraction = (
            data_frame
            if self.allow_lagged_targets
            else _remove_target_column(data_frame, self.target)
        )
        df_extracted = self._extract_features(df_for_extraction)
        df_selected = self._select_features(df_extracted, target)
        df_selected = _add_original_columns(data_frame, df_selected)
        end = time.time()
        _print_time_taken(begin, end)
        self.fitted = True
        self._runtime = datetime.timedelta(seconds=end - begin)
        return df_selected

    @property
    def runtime(self):
        if self.fitted:
            return self._runtime

    def transform(self, data_frame):
        """
        Fits the DFS on the data frame and returns
        the features for the training set.
        """
        df_for_extraction = (
            data_frame
            if self.allow_lagged_targets
            else _remove_target_column(data_frame, self.target)
        )
        df_extracted = self._extract_features(df_for_extraction)
        df_selected = df_extracted[self.selected_features]
        df_selected = _add_original_columns(data_frame, df_selected)
        return df_selected
