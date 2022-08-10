"""
Wrapper around KATS.tsfeatures
"""

import datetime
import itertools as it
import logging
import time
import warnings
from contextlib import contextmanager
from typing import Dict, Generator, List, NamedTuple, Optional, Union

import numpy as np
import pandas as pd  # type: ignore
from kats.consts import TimeSeriesData
from kats.tsfeatures.tsfeatures import TsFeatures
from pandas.api.types import is_numeric_dtype  # type: ignore
from scipy.stats import pearsonr

from .add_original_columns import _add_original_columns
from .print_time_taken import _print_time_taken
from .remove_target_column import _remove_target_column

KATSAggs = Dict[str, List[str]]

ALL_AGGS: KATSAggs = TsFeatures().feature_group_mapping


@contextmanager
def disable_logging(highest_level=logging.CRITICAL):
    previous_level = logging.root.manager.disable

    logging.disable(highest_level)

    try:
        yield
    finally:
        logging.disable(previous_level)


# ------------------------------------------------------------------


class _ChunkMaker(NamedTuple):
    """
    Helpers class to create chunks of data frames.
    """

    data_frame: pd.DataFrame
    id_col: pd.Series
    time_col: pd.Series
    horizon: pd.Timedelta
    memory: pd.Timedelta
    min_chunksize: int

    def make_chunk(self, current_id: str, now: pd.Timedelta) -> pd.DataFrame:
        """
        Generates a chunk of the data frame that
        contains all rows within horizon and memory.

        Used by roll_data_frame.
        """
        begin = now - self.horizon - self.memory
        end = now - self.horizon
        chunk = self.data_frame[
            (self.id_col == current_id)
            & (self.time_col > begin)
            & (self.time_col <= end)
        ]
        return (
            chunk
            if len(chunk) >= self.min_chunksize
            else pd.DataFrame(columns=self.data_frame.columns)
        )


def _flatten_aggs(aggs: KATSAggs, domains: Optional[List[str]] = None) -> List[str]:
    domains = domains or list(aggs.keys())
    return list(it.chain(*aggs.values()))


def _get_aggs_by_domain(aggs: KATSAggs, domain: str) -> KATSAggs:
    return {name: aggs for name, aggs in ALL_AGGS.items() if name == domain}


# ------------------------------------------------------------------


def _roll_data_frame(
    data_frame: pd.DataFrame,
    column_id: str,
    time_stamp: str,
    horizon: pd.Timedelta,
    memory: pd.Timedelta,
    min_chunksize: int,
) -> Generator[pd.DataFrame, None, None]:
    """
    Returns a generator that contains pd.DataFrame chunks
    to be aggregated.
    """
    id_col = data_frame[column_id]
    time_col = pd.to_datetime(data_frame[time_stamp])
    chunk_maker = _ChunkMaker(
        data_frame, id_col, time_col, horizon, memory, min_chunksize
    )
    return (
        chunk_maker.make_chunk(row[column_id], pd.to_datetime(row[time_stamp]))
        for _, row in data_frame.iterrows()
    )


# ------------------------------------------------------------------


def _aggregate_chunk(
    chunk: pd.DataFrame,
    column_id: str,
    time_stamp: str,
    aggregations: KATSAggs,
) -> pd.DataFrame:
    orig_colnames = [col for col in chunk.columns if col not in (time_stamp, column_id)]
    data = []
    for col in orig_colnames:
        colnames = [f"{name} ({col})" for name in _flatten_aggs(aggregations)]
        if chunk.shape[0] > 1:
            chunk_ts = TimeSeriesData(
                chunk[[col, time_stamp]].rename(columns={time_stamp: "time"})
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                with disable_logging():
                    features_chunk = pd.DataFrame(
                        data=[
                            TsFeatures(
                                selected_features=_flatten_aggs(aggregations)
                            ).transform(chunk_ts)
                        ],
                    )
                    features_chunk.columns = colnames
        else:
            features_chunk = pd.DataFrame(columns=colnames)
        data.append(features_chunk)

    return pd.concat(data, axis=1)


# ------------------------------------------------------------------


def _aggregate_chunks(
    rolled: Generator[pd.DataFrame, None, None],
    column_id: str,
    time_stamp: str,
    aggregations: KATSAggs,
) -> pd.DataFrame:
    aggregated_chunks: List[pd.DataFrame] = [
        _aggregate_chunk(chunk, column_id, time_stamp, aggregations) for chunk in rolled
    ]
    return (
        pd.concat(aggregated_chunks, ignore_index=True)
        .reset_index()
        .drop("index", axis=1)
    )


# ------------------------------------------------------------------


class KATSBuilder:
    """
    Scikit-learn-style feature builder based on TSFEL.

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

    stl_aggs: KATSAggs = _get_aggs_by_domain(ALL_AGGS, "stl_features")
    level_shift_aggs: KATSAggs = _get_aggs_by_domain(ALL_AGGS, "level_shift_features")
    holt_aggs: KATSAggs = _get_aggs_by_domain(ALL_AGGS, "holt_params")
    hw_aggs: KATSAggs = _get_aggs_by_domain(ALL_AGGS, "hw_params")
    statistics_aggs: KATSAggs = _get_aggs_by_domain(ALL_AGGS, "statistics")
    cusum_aggs: KATSAggs = _get_aggs_by_domain(ALL_AGGS, "cusum_detector")
    robust_stat_aggs: KATSAggs = _get_aggs_by_domain(ALL_AGGS, "robust_stat_detector")
    bocp_aggs: KATSAggs = _get_aggs_by_domain(ALL_AGGS, "bocp_detector")
    outlier_aggs: KATSAggs = _get_aggs_by_domain(ALL_AGGS, "outlier_detector")
    trend_aggs: KATSAggs = _get_aggs_by_domain(ALL_AGGS, "trend_detector")
    nowcasting_aggs: KATSAggs = _get_aggs_by_domain(ALL_AGGS, "nowcasting")
    seasonalities_aggs: KATSAggs = _get_aggs_by_domain(ALL_AGGS, "seasonalities")

    all_aggs: KATSAggs = {
        **stl_aggs,
        **level_shift_aggs,
        **holt_aggs,
        **hw_aggs,
        **statistics_aggs,
        **cusum_aggs,
        **robust_stat_aggs,
        **bocp_aggs,
        **outlier_aggs,
        **trend_aggs,
        **nowcasting_aggs,
        **seasonalities_aggs,
    }

    default_aggs: KATSAggs = {
        **statistics_aggs,
        **cusum_aggs,
        **seasonalities_aggs,
    }

    def __init__(
        self,
        num_features: int,
        horizon: pd.Timedelta,
        memory: pd.Timedelta,
        column_id: str,
        time_stamp: str,
        target: str,
        aggregations: Optional[KATSAggs] = None,
        allow_lagged_targets: bool = False,
        min_chunksize: int = 0,
        n_jobs: int = 1,
    ) -> None:
        self.aggregations = aggregations or self.default_aggs
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
        self.min_chunksize = min_chunksize

        self.num_features_generated = 0
        self.selected_features: List[int] = []

    def _extract_features(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        data_frame = data_frame.reset_index()
        del data_frame["index"]
        rolled = _roll_data_frame(
            data_frame,
            self.column_id,
            self.time_stamp,
            self.horizon,
            self.memory,
            self.min_chunksize,
        )
        df_extracted = _aggregate_chunks(
            rolled, self.column_id, self.time_stamp, self.aggregations
        )
        for col in df_extracted:
            try:
                df_extracted[col][df_extracted[col].isna()] = 0.0
                df_extracted[col][np.isinf(df_extracted[col])] = 0.0
            except TypeError:
                pass
        return df_extracted

    def _select_features(
        self, data_frame: pd.DataFrame, target: Union[pd.Series, np.ndarray]
    ) -> pd.DataFrame:
        colnames = np.asarray(data_frame.columns)
        print("Selecting the best out of " + str(len(colnames)) + " features...")
        colnames = np.asarray(
            [col for col in colnames if np.var(np.asarray(data_frame[col])) > 0.0]
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

    def fit(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        """
        Fits the DFS on the data frame and returns
        the features for the training set.
        """
        print(f"KATS: Trying features...")
        begin = time.time()
        target = np.asarray(data_frame[self.target])
        df_for_extraction = (
            data_frame
            if self.allow_lagged_targets
            else _remove_target_column(data_frame, self.target)
        )
        df_extracted = self._extract_features(df_for_extraction)
        cutoff = len(target) - len(df_extracted)
        df_selected = self._select_features(df_extracted, target[cutoff:])
        df_selected = _add_original_columns(
            df_for_extraction,
            df_selected,
            cutoff=df_for_extraction.shape[0] - df_selected.shape[0],
        )
        end = time.time()
        _print_time_taken(begin, end)
        self.fitted = True
        self._runtime = datetime.timedelta(seconds=end - begin)
        return df_selected

    @property
    def runtime(self) -> Union[None, datetime.timedelta]:
        if self.fitted:
            return self._runtime

    def transform(self, data_frame: pd.DataFrame) -> pd.DataFrame:
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
        cutoff = len(df_for_extraction) - len(df_selected)
        df_selected = _add_original_columns(
            df_for_extraction,
            df_selected,
            cutoff=df_for_extraction.shape[0] - df_selected.shape[0],
        )
        return df_selected
