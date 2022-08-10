import datetime
import time

import featuretools as ft
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype
from scipy.stats import pearsonr

from .add_original_columns import _add_original_columns
from .print_time_taken import _print_time_taken
from .remove_target_column import _remove_target_column

# ------------------------------------------------------------------


class _ChunkMaker:
    """
    Helpers class to create chunks of data frames.
    """

    def __init__(self, data_frame, id_col, time_col, horizon, memory):
        self.data_frame = data_frame
        self.id_col = id_col
        self.time_col = time_col
        self.horizon = horizon
        self.memory = memory

    def make_chunk(self, current_id, now, index):
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
        chunk["_featuretools_join_key"] = int(index)
        return chunk


# ------------------------------------------------------------------


def _make_entity_set(data_frame, rolled, time_stamp):
    relationships = [
        ("population", "_featuretools_index", "peripheral", "_featuretools_join_key")
    ]

    dataframes = {
        "population": (
            data_frame,
            "_featuretools_index",
            time_stamp,
        ),
        "peripheral": (
            rolled,
            "_featuretools_index",
            time_stamp,
        ),
    }

    return ft.EntitySet("self-join-entity-set", dataframes, relationships)


# ------------------------------------------------------------------


def _roll_data_frame(data_frame, column_id, time_stamp, horizon, memory):
    """
    Duplicates data so that it matches the format
    required by featuretools.
    """
    id_col = data_frame[column_id]
    time_col = pd.to_datetime(data_frame[time_stamp])
    chunk_maker = _ChunkMaker(data_frame, id_col, time_col, horizon, memory)
    chunks = [
        chunk_maker.make_chunk(row[column_id], pd.to_datetime(row[time_stamp]), index)
        for index, row in data_frame.iterrows()
    ]
    rolled = pd.concat(chunks, ignore_index=True).reset_index()
    rolled["_featuretools_index"] = np.arange(rolled.shape[0])
    return rolled


# ------------------------------------------------------------------


class FTTimeSeriesBuilder:
    """
    Scikit-learn-style feature builder based on featuretools.

    Args:

        num_features: The (maximum) number of features to build.

        memory: How much back in time you want to go until the
                feature builder starts "forgetting" data.

        column_id: The name of the column containing the ids.

        time_stamp: The name of the column containing the time stamps.

        target: The name of the target column.
    """

    all_primitives = ft.list_primitives()
    agg_primitives = all_primitives[all_primitives.type == "aggregation"].name.tolist()
    trans_primitives = all_primitives[all_primitives.type == "transform"].name.tolist()

    def __init__(
        self,
        num_features,
        horizon,
        memory,
        column_id,
        time_stamp,
        target,
        allow_lagged_targets=False,
        min_chunksize=0,
        n_jobs=1,
    ):
        self.num_features = num_features
        self.horizon = horizon
        self.memory = memory
        self.column_id = column_id
        self.time_stamp = time_stamp
        self.target = target
        self.allow_lagged_targets = allow_lagged_targets
        self.n_jobs = n_jobs

        self._runtime = None
        self.fitted = False
        self.max_depth = 2

        self.num_features_generated = 0
        self.selected_features = []

    def _extract_features(self, data_frame):
        data_frame = data_frame.reset_index()
        del data_frame["index"]
        rolled = _roll_data_frame(
            data_frame, self.column_id, self.time_stamp, self.horizon, self.memory
        )

        data_frame["_featuretools_index"] = np.arange(data_frame.shape[0])

        entityset = _make_entity_set(data_frame, rolled, self.time_stamp)
        df_extracted, _ = ft.dfs(
            entityset=entityset,
            agg_primitives=self.agg_primitives,
            target_dataframe_name="population",
            max_depth=self.max_depth,
            ignore_columns={
                "peripheral": [
                    self.column_id,
                    "index",
                    "join_key",
                    "_featuretools_join_key",
                    "_featuretools_index",
                ]
            },
            n_jobs=self.n_jobs,
        )

        for col in df_extracted:
            if is_numeric_dtype(df_extracted[col]):
                df_extracted[col][df_extracted[col].isna()] = 0

        return df_extracted

    def _select_features(self, data_frame, target):
        colnames = np.asarray(data_frame.columns)
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
        print("featuretools: Trying features...")
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
