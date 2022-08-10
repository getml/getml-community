"""
Utility wrapper around tsfresh.
"""

import datetime
import gc
import time
from typing import List, Optional

import numpy as np
import pandas as pd
import tsfresh
from tsfresh.utilities.dataframe_functions import roll_time_series
from scipy.stats import pearsonr

from .add_original_columns import _add_original_columns
from .print_time_taken import _print_time_taken


def _ts_to_float(col):
    epoch = datetime.datetime(1970, 1, 1)
    return [(t - epoch).total_seconds() for t in col]


class TSFreshBuilder:
    """
    Scikit-learn-style feature builder based on TSFresh.

    Args:

        num_features: The (maximum) number of features to build.

        memory: How much back in time you want to go until the
                feature builder starts "forgetting" data.

        column_id: The name of the column containing the ids.

        time_stamp: The name of the column containing the time stamps.

        target: The name of the target column.
    """

    def __init__(
        self,
        num_features,
        memory,
        column_id,
        time_stamp,
        target,
        horizon=0,
        allow_lagged_targets=False,
        min_chunksize: int = 0,
        n_jobs: int = 1,
    ):
        self.num_features = num_features
        self.memory = memory
        self.column_id = column_id
        self.time_stamp = time_stamp
        self.target = target
        self.horizon = horizon
        self.allow_lagged_targets = allow_lagged_targets

        self._runtime = None

        self.num_features_generated = 0
        self.selected_features: List[int] = []

    def _extract_features(self, data_frame):
        # https://github.com/blue-yonder/tsfresh/issues/716#issuecomment-649755423
        df_numerical = data_frame.select_dtypes(include=[np.number])

        df_numerical[self.time_stamp] = _ts_to_float(data_frame[self.time_stamp])

        df_rolled = roll_time_series(
            df_numerical,
            column_id=self.column_id,
            column_sort=self.time_stamp,
            max_timeshift=self.memory,
        )

        df_rolled = df_rolled[
            [col for col in df_rolled.columns if col != self.column_id]
        ]

        extracted_minimal = tsfresh.extract_features(
            df_rolled,
            column_id="id",
            column_sort=self.time_stamp,
            default_fc_parameters=tsfresh.feature_extraction.MinimalFCParameters(),
            n_jobs=1,
        )

        extracted_index_based = tsfresh.extract_features(
            df_rolled,
            column_id="id",
            column_sort=self.time_stamp,
            default_fc_parameters=tsfresh.feature_extraction.settings.IndexBasedFCParameters(),
            n_jobs=1,
        )

        extracted_features = pd.concat(
            [extracted_minimal, extracted_index_based], axis=1
        )
        del extracted_minimal
        del extracted_index_based

        gc.collect()

        extracted_features[np.isnan(extracted_features)] = 0.0

        extracted_features[np.isinf(extracted_features)] = 0.0

        return extracted_features

    def _remove_target_column(self, data_frame):
        colnames = np.asarray(data_frame.columns)

        if self.target not in colnames:
            return data_frame

        colnames = colnames[colnames != self.target]

        return data_frame[colnames]

    def _select_features(self, data_frame, target):
        print(
            "Selecting the best out of " + str(len(data_frame.columns)) + " features..."
        )

        df_selected = tsfresh.select_features(data_frame, target)

        colnames = np.asarray(df_selected.columns)

        correlations = np.asarray(
            [np.abs(pearsonr(target, df_selected[col]))[0] for col in colnames]
        )

        # [::-1] is somewhat unintuitive syntax,
        # but it reverses the entire column.
        self.selected_features = colnames[np.argsort(correlations)][::-1][
            : self.num_features
        ]

        self.num_features_generated = data_frame.shape[1]
        return df_selected[self.selected_features]

    def fit(self, data_frame):
        """
        Fits the features.
        """
        begin = time.time()

        target = np.asarray(data_frame[self.target])

        df_for_extraction = (
            data_frame
            if self.allow_lagged_targets
            else self._remove_target_column(data_frame)
        )

        df_extracted = self._extract_features(df_for_extraction)

        df_selected = self._select_features(df_extracted, target)

        del df_extracted
        gc.collect()

        df_selected = _add_original_columns(data_frame, df_selected)

        end = time.time()

        self._runtime = datetime.timedelta(seconds=end - begin)

        _print_time_taken(begin, end)

        return df_selected

    @property
    def runtime(self):
        return self._runtime

    def transform(self, data_frame):
        """
        Transforms the raw data into a set of features.
        """
        df_for_extraction = (
            data_frame
            if self.allow_lagged_targets
            else self._remove_target_column(data_frame)
        )

        df_extracted = self._extract_features(df_for_extraction)

        df_selected = df_extracted[self.selected_features]

        del df_extracted
        gc.collect()

        df_selected = _add_original_columns(data_frame, df_selected)

        return df_selected
