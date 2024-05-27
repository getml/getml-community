import logging
from datetime import timedelta
from typing import Any, Union

from getml.data.time_series import TimeSeries
from getml.feature_learning.fastprop import FastProp
from getml.feature_learning.loss_functions import SquareLoss
from getml.pipeline.pipeline import Pipeline
from getml.predictors.xgboost_regressor import XGBoostRegressor
from getml.preprocessors import Seasonal

from benchmarks.demo import Benchmark


def drive_getml(
    observer: Benchmark,
    time_series: TimeSeries,
    frequency: timedelta,
    num_features: int = 1000,
    min_chunksize: int = 0,
    n_jobs: int = 1,
) -> Benchmark:
    """
    A common interface for benchmarking propositionalization on time series with getml.
    """

    fast_prop = FastProp(
        aggregation=FastProp.agg_sets.All,
        loss_function=SquareLoss,
        num_features=num_features,
        num_threads=n_jobs,
    )

    pipe_fl = Pipeline(
        feature_learners=[fast_prop],
        data_model=time_series.data_model,
        tags=["fastprop", "paper", "benchmark"],
    )

    features = {}

    pipe_fl.check(time_series.train)

    with observer.benchmark("fastprop"):
        pipe_fl.fit(time_series.train)
        features["train"] = pipe_fl.transform(
            time_series.train, df_name="features_train"
        )

    features["test"] = pipe_fl.transform(time_series.test, df_name="features_test")

    if min_chunksize > 0:
        features["train"] = features["train"][min_chunksize:]

    pipe_pr = Pipeline(
        tags=["prediction", "fastprop"],
        predictors=XGBoostRegressor(),
    )

    pipe_pr.fit(features["train"])
    pipe_pr.score(features["test"])

    num_features = len(
        pipe_pr.features.filter(lambda feat: feat.name.startswith("feature_"))
    )

    observer.write_scores("fastprop", pipe_pr, num_features)

    return observer
