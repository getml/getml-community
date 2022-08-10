# Copyright 2022 The SQLNet Company GmbH
# 
# This file is licensed under the Elastic License 2.0 (ELv2). 
# Refer to the LICENSE.txt file in the root of the repository 
# for details.
# 

"""
This is an integration test based on
the loans dataset. The purpose is
to make sure that saving and loading pipelines
works as expected.
"""

import numpy as np

import getml as getml


def test_save_load():
    """
    This is an integration test based on
    the loans dataset. The purpose is
    to make sure that saving and loading pipelines
    works as expected.
    """
    getml.engine.launch()
    getml.set_project("test_save_load")

    star_schema = _make_star_schema()

    fast_prop = (
        getml.feature_learning.FastProp(  # pylint: disable=unexpected-keyword-arg
            aggregation=getml.feature_learning.FastProp.agg_sets.All,
            loss_function=getml.feature_learning.loss_functions.CrossEntropyLoss,
            num_threads=1,
        )
    )

    feature_selector = getml.predictors.XGBoostClassifier(n_jobs=1)

    predictor = getml.predictors.XGBoostClassifier(
        gamma=2,
        n_jobs=1,
    )

    pipe1 = getml.pipeline.Pipeline(
        data_model=star_schema.data_model,
        feature_learners=[fast_prop],
        feature_selectors=[feature_selector],
        predictors=predictor,
    )

    pipe1.fit(star_schema.train)

    predictions1 = pipe1.predict(star_schema.test)
    sql_code1 = pipe1.features.to_sql()

    getml.engine.suspend_project("test_save_load")
    getml.engine.set_project("test_save_load")

    star_schema = _make_star_schema()

    pipe2 = getml.pipeline.load(pipe1.id)
    predictions2 = pipe2.predict(star_schema.test)
    sql_code2 = pipe2.features.to_sql()

    assert np.allclose(predictions1, predictions2), "Must be the same"
    assert sql_code1.to_str() == sql_code2.to_str(), "Must be the same"

    getml.engine.delete_project("test_save_load")


def _make_star_schema() -> getml.data.StarSchema:
    population_train, population_test, order, trans, meta = getml.datasets.load_loans(
        roles=True, units=True
    )

    star_schema = getml.data.StarSchema(
        train=population_train, test=population_test, alias="population"
    )

    star_schema.join(
        trans,
        on="account_id",
        time_stamps=("date_loan", "date"),
    )

    star_schema.join(
        order,
        on="account_id",
    )

    star_schema.join(
        meta,
        on="account_id",
    )

    return star_schema
