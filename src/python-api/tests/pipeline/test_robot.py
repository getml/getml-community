# Copyright 2022 The SQLNet Company GmbH
# 
# This file is licensed under the Elastic License 2.0 (ELv2). 
# Refer to the LICENSE.txt file in the root of the repository 
# for details.
# 

"""
This is an integration test based on
the robot dataset.
"""


import getml as getml


def test_robot():
    """
    This is an integration test based on
    the robot dataset.
    """

    getml.engine.launch()
    getml.engine.set_project("robot")

    data_all = getml.data.DataFrame.from_csv(
        "https://static.getml.com/datasets/robotarm/robot-demo.csv", "data_all"
    )

    data_all.set_role(["f_x", "f_y", "f_z"], getml.data.roles.target)
    data_all.set_role(data_all.roles.unused, getml.data.roles.numerical)

    split = getml.data.split.time(data_all, "rowid", test=10500)

    time_series = getml.data.TimeSeries(
        population=data_all,
        split=split,
        time_stamps="rowid",
        lagged_targets=False,
        memory=30,
    )

    fast_prop = (
        getml.feature_learning.FastProp(  # pylint: disable=unexpected-keyword-arg
            loss_function=getml.feature_learning.loss_functions.SquareLoss,
            num_features=10,
        )
    )

    xgboost = getml.predictors.XGBoostRegressor()

    pipe1 = getml.pipeline.Pipeline(
        data_model=time_series.data_model,
        feature_learners=[fast_prop],
        predictors=xgboost,
    )

    pipe1.check(time_series.train)
    pipe1.fit(time_series.train)

    scores = pipe1.score(time_series.test)

    assert all(
        (r > 0.987 for r in scores.rsquared)
    ), "Expected all r-squared values to be greater than 0.987, got " + str(
        scores.rsquared
    )
