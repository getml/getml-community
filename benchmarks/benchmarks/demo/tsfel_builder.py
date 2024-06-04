"""
Wrapper around TSFEL
"""

import datetime
import time
import warnings
from typing import Dict, Iterator, List, Optional, TypedDict, Union

import numpy as np
import pandas as pd  # type: ignore
from pandas.api.types import is_numeric_dtype  # type: ignore
from tsfel.feature_extraction.calc_features import calc_window_features
from tsfel.feature_extraction.features_settings import (
    get_features_by_domain,  # type: ignore
)

from .print_time_taken import _print_time_taken
from .rolling import _roll_data_frame


class TSFELAgg(TypedDict):
    """
    A typed dict holding meta data for a certain TSFEL aggregation.
    """

    complexity: str
    description: str
    function: str
    parameters: Union[str, Dict[str, Union[str, int]]]
    n_features: int
    use: str
    tag: Union[str, List[str]]


TSFELAggs = Dict[str, Dict[str, TSFELAgg]]

# ------------------------------------------------------------------


def _aggregate_chunk(
    chunk: pd.DataFrame,
    column_id: str,
    time_stamp: str,
    aggregations: TSFELAggs,
    n_jobs: Optional[int],
) -> pd.DataFrame:
    orig_colnames = [col for col in chunk.columns if col not in (time_stamp, column_id)]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return calc_window_features(
            signal_window=chunk[orig_colnames],
            dict_features=aggregations,
            window_size=len(chunk),
            verbose=0,
            fs=100,
            n_jobs=n_jobs,
        )


# -------------------------------------------------------------------


def _aggregate_chunks(
    rolled: Iterator[pd.DataFrame],
    column_id: str,
    time_stamp: str,
    aggregations: TSFELAggs,
    n_jobs: Optional[int],
) -> pd.DataFrame:
    aggregated_chunks: List[pd.DataFrame] = [
        _aggregate_chunk(chunk, column_id, time_stamp, aggregations, n_jobs)
        for chunk in rolled
        if len(chunk) > 5
    ]
    return pd.concat(aggregated_chunks, ignore_index=True).reset_index(drop=True)


# -------------------------------------------------------------------


def _flatten_aggs(
    aggs: TSFELAggs, domains: Optional[List[str]] = None
) -> Dict[str, TSFELAgg]:
    domains = domains or list(aggs.keys())
    return {
        name: {**values, **{"domain": domain}}
        for domain, agg in aggs.items()
        for name, values in agg.items()
        if domain in domains
    }


# ------------------------------------------------------------------


# TODO: Complete? Check for additional params
def _infer_required_obs(aggs: TSFELAggs) -> int:
    try:
        n_coeff = int(_flatten_aggs(aggs)["LPCC"]["parameters"]["n_coeff"])
    except KeyError:
        n_coeff = 1

    try:
        d = int(_flatten_aggs(aggs)["ECDF"]["parameters"]["d"])
    except KeyError:
        d = 1

    return max(n_coeff, d)


# ------------------------------------------------------------------


class TSFELBuilder:
    """
    Scikit-learn-style feature builder based on TSFEL.

    Args:
        num_features: The (maximum) number of features to build.

        horizon: The prediction horizon to use.

        memory: How much back in time you want to go until the
                feature builder starts "forgetting" data.

        column_id: The name of the column containing the ids.

        time_stamp: The name of the column containing the time stamps.

        target: The name of the target column.

        aggregations: The aggregations to use.

        allow_lagged_targets: Whether to build features based on lagged targets

        min_chunksize: The minimum size of chunks to aggregate over

    """

    statistical_aggs: TSFELAggs = get_features_by_domain("statistical")
    temporal_aggs: TSFELAggs = get_features_by_domain("temporal")
    # spectral_aggs: TSFELAggs = tsfel.get_features_by_domain("spectral")
    # all_aggs: TSFELAggs = tsfel.get_features_by_domain()
    all_aggs: TSFELAggs = {**statistical_aggs, **temporal_aggs}

    def __init__(
        self,
        num_features: int,
        horizon: pd.Timedelta,
        memory: pd.Timedelta,
        column_id: str,
        time_stamp: str,
        target: str,
        aggregations: Optional[TSFELAggs] = None,
        allow_lagged_targets: bool = False,
        min_chunksize: Optional[int] = None,
        n_jobs: Optional[int] = None,
    ) -> None:
        self.num_features = num_features
        self.horizon = horizon
        self.memory = memory
        self.column_id = column_id
        self.time_stamp = time_stamp
        self.target = target
        self.aggregations = aggregations or self.all_aggs
        self.allow_lagged_targets = allow_lagged_targets
        self.min_chunksize = min_chunksize or _infer_required_obs(self.aggregations)
        self.n_jobs = None if n_jobs == 1 else n_jobs

        self._runtime = None
        self.fitted = False

        self.num_features_generated = 0
        self.selected_features: List[int] = []

    def _extract_features(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        data_frame = data_frame.select_dtypes(
            include=[np.number, np.datetime64]
        ).reset_index(drop=True)
        rolled = _roll_data_frame(
            data_frame, self.time_stamp, self.horizon, self.memory, self.min_chunksize
        )
        extracted = _aggregate_chunks(
            rolled, self.column_id, self.time_stamp, self.aggregations, self.n_jobs
        )
        for col in extracted:
            if is_numeric_dtype(extracted[col]):
                extracted[col][extracted[col].isna()] = 0
        return extracted

    def _select_features(
        self, data_frame: pd.DataFrame, target: Union[pd.Series, np.ndarray]
    ) -> pd.DataFrame:
        print(f"Selecting the best out of {data_frame.shape[1]} features...")

        if not self.selected_features:
            data_frame = data_frame.loc[:, (data_frame != data_frame.iloc[0]).any()]
            correlations = data_frame.corrwith(pd.Series(target)).abs()
            correlations = correlations.replace([np.inf, np.nan], 0)
            correlations = correlations.sort_values(ascending=False)
            self.selected_features = correlations.index[: self.num_features].tolist()

        self.num_features_generated = data_frame.shape[1]
        return data_frame[self.selected_features]

    def fit(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        """
        Fits the DFS on the data frame and returns
        the features for the training set.
        """
        print("TSFEL: Trying features...")
        begin = time.time()
        grouped = data_frame.groupby(self.column_id)
        extracted = []
        other = []
        for _, group in grouped:
            to_extract = group if self.allow_lagged_targets else group.drop(self.target)
            extracted.append(self._extract_features(to_extract))
            other.append(group[self.min_chunksize :])
        extracted = pd.concat(extracted)
        other = pd.concat(other).reset_index(drop=True)
        selected = self._select_features(extracted, other[self.target])
        selected = pd.concat([selected, other], axis=1)
        end = time.time()
        _print_time_taken(begin, end)
        self.fitted = True
        self._runtime = datetime.timedelta(seconds=end - begin)
        return selected

    @property
    def runtime(self) -> Union[None, datetime.timedelta]:
        if self.fitted:
            return self._runtime

    def transform(self, data_frame):
        """
        Transforms the raw data into a set of features.
        """

        return self.fit(data_frame)
