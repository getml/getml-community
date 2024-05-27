"""
Wrapper around tsflex
"""

import datetime
import time
import warnings
from typing import Callable, List, Optional, Union, cast

import numpy as np
import pandas as pd  # type: ignore
from pandas.api.types import is_numeric_dtype  # type: ignore
from tsflex.features import FeatureCollection, MultipleFeatureDescriptors
from tsflex.features.function_wrapper import FuncWrapper

from .print_time_taken import _print_time_taken
from .tsfel_builder import TSFELBuilder
from .tsflex_aggregations import make_fastprop_aggregations

# ------------------------------------------------------------------


def _wrap_aggregation(func) -> Callable:
    return FuncWrapper(
        func=func,
        output_names=func.__name__,
    )


# ------------------------------------------------------------------


class TsflexBuilder:
    """
    Scikit-learn-style feature builder based on tsflex.

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

        n_jobs: TODO
    """

    def __init__(
        self,
        num_features: int,
        horizon: pd.Timedelta,
        memory: pd.Timedelta,
        column_id: str,
        time_stamp: str,
        target: str,
        strides: pd.Timedelta,
        aggregations: Optional[List[Callable]] = None,
        allow_lagged_targets: bool = False,
        min_chunksize: int = 0,
        n_jobs: int = 1,
    ) -> None:
        self.num_features = num_features
        self.horizon = horizon
        self.memory = memory
        self.column_id = column_id
        self.time_stamp = time_stamp
        self.target = target
        self.strides = strides
        self.aggregations = aggregations or make_fastprop_aggregations(
            float(strides.total_seconds())
        )
        self.allow_lagged_targets = allow_lagged_targets
        self.min_chunksize = min_chunksize
        self.n_jobs = n_jobs

        self._runtime = None
        self.fitted = False

        self.selected_features: List[int] = []
        self.num_features_generated = 0

    def _extract_features(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        fc = FeatureCollection(
            MultipleFeatureDescriptors(
                functions=[_wrap_aggregation(agg) for agg in self.aggregations],
                series_names=[
                    col for col in data_frame.columns if col != self.column_id
                ],
                windows=self.memory,
                strides=self.strides,
            )
        )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            extracted = cast(
                pd.DataFrame, fc.calculate([data_frame], return_df=True, n_jobs=1)
            )

        for col in extracted:
            if is_numeric_dtype(extracted[col]):
                extracted[col][extracted[col].isna()] = 0

        return extracted

    def _select_features(
        self, data_frame: pd.DataFrame, target: Union[pd.Series, np.ndarray]
    ) -> pd.DataFrame:
        print(f"Selecting the best out of {data_frame.shape[1]} features...")

        data_frame = data_frame.loc[:, (data_frame != data_frame.iloc[0]).any()]
        correlations = data_frame.corrwith(pd.Series(target)).abs()
        correlations = correlations.replace([np.inf, np.nan], 0)
        correlations = correlations.sort_values(ascending=False)

        self.num_features_generated = data_frame.shape[1]
        self.selected_features = correlations.index[: self.num_features].tolist()
        return data_frame[self.selected_features]

    def fit(
        self, data_frame: pd.DataFrame, selection: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Fits the DFS on the data frame and returns
        the features for the training set.
        """
        print("tsflex: Trying features...")
        begin = time.time()
        data_frame = data_frame.set_index(self.time_stamp)
        grouped = data_frame.groupby(self.column_id)
        extracted = []
        other = []
        for _, group in grouped:
            to_extract = group if self.allow_lagged_targets else group.drop(self.target)
            extracted.append(self._extract_features(to_extract))
            other.append(group[self.min_chunksize :])
        extracted = pd.concat(extracted)
        other = pd.concat(other)
        if selection:
            selected = extracted[selection]
        else:
            selected = self._select_features(extracted, other[self.target])
        selected = pd.concat([selected, other], axis=1)
        selected = selected[selected["id_"] == selected["id_"]]
        end = time.time()
        _print_time_taken(begin, end)

        self.fitted = True
        self._runtime = datetime.timedelta(seconds=end - begin)
        return selected

    @property
    def runtime(self) -> Union[None, datetime.timedelta]:
        if self.fitted:
            return self._runtime

    def transform(
        self, data_frame: pd.DataFrame, data_frame_full: Optional[pd.DataFrame] = None
    ):
        """
        Transforms the raw data into a set of features.
        """

        if not self.fitted:
            raise RuntimeError

        if data_frame_full is not None:
            split_ix = np.argmax(
                data_frame_full[self.time_stamp] == data_frame[self.time_stamp][0]
            )
            data_frame = data_frame_full[split_ix - self.min_chunksize :]

        return self.fit(data_frame, selection=self.selected_features)
