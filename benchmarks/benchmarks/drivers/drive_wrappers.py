from datetime import timedelta
from typing import Optional, Type, Union

import numpy as np
import pandas as pd  # type: ignore
from benchmarks.demo import (
    Benchmark,
    CesiumMLBuilder,
    KATSBuilder,
    FTTimeSeriesBuilder,
    TSFELBuilder,
    TsflexBuilder,
    TSFreshBuilder,
)

from getml.data import Container, DataFrame, TimeSeries, View, roles
from getml.data.columns.columns import StringColumnView
from getml.pipeline.pipeline import Pipeline
from getml.predictors.xgboost_regressor import XGBoostRegressor


def _drive_wrapper(
    wrapper: Type[
        Union[FTTimeSeriesBuilder, TSFreshBuilder, TSFELBuilder, TsflexBuilder]
    ],
    lib_name: str,
    observer: Benchmark,
    time_series: TimeSeries,
    min_chunksize: int = 0,
    num_features: int = 1000,
    n_jobs: Optional[int] = 1,
    transform_horizon_memory: bool = True,
    strides: Optional[pd.Timedelta] = None,
) -> Benchmark:

    dfs_pd = time_series.container.to_pandas()

    horizon = (
        pd.Timedelta(time_series.horizon, unit="S")
        if transform_horizon_memory
        else time_series.horizon
    )
    memory = (
        pd.Timedelta(time_series.memory, unit="S")
        if transform_horizon_memory
        else time_series.memory
    )
    kwargs = {"strides": strides} if strides is not None else {}
    wrapper_instance = wrapper(
        horizon=horizon,
        memory=memory,
        column_id="id_",
        time_stamp=time_series.time_stamps,
        target=time_series.population.roles.target[0],
        num_features=num_features,
        allow_lagged_targets=time_series.lagged_targets,
        min_chunksize=min_chunksize,
        n_jobs=n_jobs,
        **kwargs
    )

    features_pd = {}

    with observer.benchmark(lib_name):
        features_pd["train"] = wrapper_instance.fit(dfs_pd["train"].assign(id_=1))

    features_pd["test"] = wrapper_instance.transform(dfs_pd["test"].assign(id_=1))

    xgboost = XGBoostRegressor()
    pipe_pr = Pipeline(tags=["prediction", lib_name], predictors=[xgboost])

    features = {
        subset: DataFrame.from_pandas(
            feat_df_pd,
            name=subset,
            roles={
                k: v
                for k, v in time_series.population.roles.to_dict().items()
                if "unused" not in k
            },
        )
        for subset, feat_df_pd in features_pd.items()
    }

    for subset in features:
        features[subset].set_role(features[subset].roles.unused, roles.numerical)

    pipe_pr.fit(features["train"])
    pipe_pr.score(features["test"])

    observer.write_scores(lib_name, pipe_pr, wrapper_instance.num_features_generated)

    return observer


def drive_featuretools(
    observer: Benchmark,
    time_series: TimeSeries,
    frequency: float,
    min_chunksize: int = 0,
    num_features: int = 1000,
    n_jobs: int = 1,
) -> Benchmark:
    """
    A common interface for benchmarking propositionalization on time series with
    featuretools.
    """

    return _drive_wrapper(
        FTTimeSeriesBuilder,
        "featuretools",
        observer,
        time_series,
        min_chunksize,
        num_features,
        n_jobs,
    )


def drive_kats(
    observer: Benchmark,
    time_series: TimeSeries,
    frequency: float,
    min_chunksize: int = 0,
    num_features: int = 1000,
    n_jobs: int = 1,
) -> Benchmark:
    """
    A common interface for benchmarking propositionalization on time series with KATS.
    """

    return _drive_wrapper(
        KATSBuilder,
        "kats",
        observer,
        time_series,
        min_chunksize,
        num_features,
        n_jobs,
    )


def drive_tsfresh(
    observer: Benchmark,
    time_series: TimeSeries,
    frequency: float,
    min_chunksize: int = 0,
    num_features: int = 1000,
    n_jobs: int = 1,
) -> Benchmark:
    """
    A common interface for benchmarking propositionalization on time series with
    tsfresh.
    """
    new_time_series = TimeSeries(
        population=time_series.population,
        split=time_series.split,
        alias=time_series.alias,
        time_stamps=time_series.time_stamps,
        horizon=int(time_series.horizon / frequency),
        memory=int(time_series.memory / frequency),
        lagged_targets=time_series.lagged_targets,
    )
    return _drive_wrapper(
        TSFreshBuilder,
        "tsfresh",
        observer,
        new_time_series,
        num_features,
        n_jobs,
        transform_horizon_memory=False,
    )


def drive_tsfel(
    observer: Benchmark,
    time_series: TimeSeries,
    frequency: float,
    min_chunksize: int = 0,
    num_features: int = 1000,
    n_jobs: int = 1,
) -> Benchmark:
    """
    A common interface for benchmarking propositionalization on time series with tsfel.
    """

    return _drive_wrapper(
        TSFELBuilder,
        "tsfel",
        observer,
        time_series,
        min_chunksize,
        num_features,
        n_jobs,
    )


def drive_tsflex(
    observer: Benchmark,
    time_series: TimeSeries,
    frequency: float,
    min_chunksize: int = 0,
    num_features: int = 1000,
    n_jobs: int = 1,
) -> Benchmark:
    """
    A common interface for benchmarking propositionalization on time series with tsflex.
    """
    strides = pd.Timedelta(seconds=frequency)
    print(min_chunksize)
    return _drive_wrapper(
        TsflexBuilder,
        "tsflex",
        observer,
        time_series,
        min_chunksize,
        num_features,
        n_jobs,
        strides=strides,
    )
