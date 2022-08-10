# Copyright 2022 The SQLNet Company GmbH
# 
# This file is licensed under the Elastic License 2.0 (ELv2). 
# Refer to the LICENSE.txt file in the root of the repository 
# for details.
# 

"""
This is an integration test based on
the Interstate94 dataset.
"""


import getml as getml


def test_interstate94():
    """
    This is an integration test based on
    the Interstate94 dataset.
    """

    getml.engine.launch()
    getml.set_project("interstate94")

    traffic = getml.datasets.load_interstate94(roles=False, units=False)

    traffic.set_role("ds", getml.data.roles.time_stamp)
    traffic.set_role("traffic_volume", getml.data.roles.target)
    traffic.set_role("holiday", getml.data.roles.categorical)

    split = getml.data.split.time(
        traffic, "ds", test=getml.data.time.datetime(2018, 3, 15)
    )

    seasonal = getml.preprocessors.Seasonal()

    time_series = getml.data.TimeSeries(
        population=traffic,
        split=split,
        time_stamps="ds",
        horizon=getml.data.time.hours(1),
        memory=getml.data.time.days(7),
        lagged_targets=True,
    )

    fast_prop = (
        getml.feature_learning.FastProp(  # pylint: disable=unexpected-keyword-arg
            loss_function=getml.feature_learning.loss_functions.SquareLoss,
            num_threads=1,
            num_features=20,
        )
    )

    predictor = getml.predictors.XGBoostRegressor()

    pipe = getml.pipeline.Pipeline(
        tags=["memory: 7d", "horizon: 1h", "fast_prop"],
        data_model=time_series.data_model,
        preprocessors=[seasonal],
        feature_learners=[fast_prop],
        predictors=[predictor],
    )

    pipe.fit(time_series.train)

    scores = pipe.score(time_series.test)

    assert scores.rsquared > 0.981, "Expected an r-squared greater 0.981, got " + str(
        scores.rsquared
    )
