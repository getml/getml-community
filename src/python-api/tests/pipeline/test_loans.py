# Copyright 2022 The SQLNet Company GmbH
# 
# This file is licensed under the Elastic License 2.0 (ELv2). 
# Refer to the LICENSE.txt file in the root of the repository 
# for details.
# 

"""
This is an integration test based on
the loans dataset.
"""


import getml as getml


def test_loans():
    """
    This is an integration test based on
    the loans dataset.
    """
    getml.engine.launch()
    getml.set_project("loans")

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

    pipe = getml.pipeline.Pipeline(
        data_model=star_schema.data_model,
        feature_learners=[fast_prop],
        feature_selectors=[feature_selector],
        predictors=predictor,
    )

    pipe.fit(star_schema.train)

    scores = pipe.score(star_schema.test)

    assert scores.auc > 0.94, "Expected an AUC greater 0.94, got " + str(scores.auc)


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
