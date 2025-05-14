# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Automatically find the best parameters for

* [`Multirel`][getml.feature_learning.Multirel]
* [`Relboost`][getml.feature_learning.Relboost]
* [`RelMT`][getml.feature_learning.RelMT]
* [`FastProp`][getml.feature_learning.FastProp]
* [`FastBoost`][getml.feature_learning.Fastboost]
* [`LinearRegression`][getml.predictors.LinearRegression]
* [`LogisticRegression`][getml.predictors.LogisticRegression]
* [`XGBoostClassifier`][getml.predictors.XGBoostClassifier]
* [`XGBoostRegressor`][getml.predictors.XGBoostRegressor]

enterprise-adm: Enterprise edition
    This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

    For licensing information and technical support, please [contact us][contact-page].

??? example
    The easiest way to conduct a hyperparameter optimization
    is to use the built-in tuning routines. Note that these
    tuning routines usually take a day to complete unless
    we use very small data sets as we do in this example.

    ```python
    from getml import data
    from getml import datasets
    from getml import engine
    from getml import feature_learning
    from getml.feature_learning import aggregations
    from getml.feature_learning import loss_functions
    from getml import hyperopt
    from getml import pipeline
    from getml import predictors

    # ----------------

    engine.set_project("examples")

    # ----------------

    population_table, peripheral_table = datasets.make_numerical()

    # ----------------
    # Construct placeholders

    population_placeholder = data.Placeholder("POPULATION")
    peripheral_placeholder = data.Placeholder("PERIPHERAL")
    population_placeholder.join(peripheral_placeholder, "join_key", "time_stamp")

    # ----------------

    feature_learner1 = feature_learning.Multirel(
        aggregation=[
            aggregations.COUNT,
            aggregations.SUM
        ],
        loss_function=loss_functions.SquareLoss,
        num_features=10,
        share_aggregations=1.0,
        max_length=1,
        num_threads=0
    )

    # ----------------

    feature_learner2 = feature_learning.Relboost(
        loss_function=loss_functions.SquareLoss,
        num_features=10
    )

    # ----------------

    predictor = predictors.LinearRegression()

    # ----------------

    pipe = pipeline.Pipeline(
        population=population_placeholder,
        peripheral=[peripheral_placeholder],
        feature_learners=[feature_learner1, feature_learner2],
        predictors=[predictor]
    )

    # ----------------

    tuned_pipeline = getml.hyperopt.tune_feature_learners(
        pipeline=base_pipeline,
        population_table_training=population_table,
        population_table_validation=population_table,
        peripheral_tables=[peripheral_table]
    )

    # ----------------

    tuned_pipeline = getml.hyperopt.tune_predictors(
        pipeline=tuned_pipeline,
        population_table_training=population_table,
        population_table_validation=population_table,
        peripheral_tables=[peripheral_table]
    )
    ```
        If you want to define the hyperparameter space and
        the tuning routing yourself, this is how you
        can do that:


    ```python
    from getml import data
    from getml import datasets
    from getml import engine
    from getml import feature_learning
    from getml.feature_learning import aggregations
    from getml.feature_learning import loss_functions
    from getml import hyperopt
    from getml import pipeline
    from getml import predictors

    # ----------------

    engine.set_project("examples")

    # ----------------

    population_table, peripheral_table = datasets.make_numerical()

    # ----------------
    # Construct placeholders

    population_placeholder = data.Placeholder("POPULATION")
    peripheral_placeholder = data.Placeholder("PERIPHERAL")
    population_placeholder.join(peripheral_placeholder, "join_key", "time_stamp")

    # ----------------
    # Base model - any parameters not included
    # in param_space will be taken from this.

    feature_learner1 = feature_learning.Multirel(
        aggregation=[
            aggregations.COUNT,
            aggregations.SUM
        ],
        loss_function=loss_functions.SquareLoss,
        num_features=10,
        share_aggregations=1.0,
        max_length=1,
        num_threads=0
    )

    # ----------------
    # Base model - any parameters not included
    # in param_space will be taken from this.

    feature_learner2 = feature_learning.Relboost(
        loss_function=loss_functions.SquareLoss,
        num_features=10
    )

    # ----------------
    # Base model - any parameters not included
    # in param_space will be taken from this.

    predictor = predictors.LinearRegression()

    # ----------------

    pipe = pipeline.Pipeline(
        population=population_placeholder,
        peripheral=[peripheral_placeholder],
        feature_learners=[feature_learner1, feature_learner2],
        predictors=[predictor]
    )

    # ----------------
    # Build a hyperparameter space.
    # We have two feature learners and one
    # predictor, so this is how we must
    # construct our hyperparameter space.
    # If we only wanted to optimize the predictor,
    # we could just leave out the feature_learners.

    param_space = {
        "feature_learners": [
            {
                "num_features": [10, 50],
            },
            {
                "max_depth": [1, 10],
                "min_num_samples": [100, 500],
                "num_features": [10, 50],
                "reg_lambda": [0.0, 0.1],
                "shrinkage": [0.01, 0.4]
            }],
        "predictors": [
            {
                "reg_lambda": [0.0, 10.0]
            }
        ]
    }

    # ----------------
    # Wrap a GaussianHyperparameterSearch around the reference model

    gaussian_search = hyperopt.GaussianHyperparameterSearch(
        pipeline=pipe,
        param_space=param_space,
        n_iter=30,
        score=pipeline.scores.rsquared
    )

    gaussian_search.fit(
        population_table_training=population_table,
        population_table_validation=population_table,
        peripheral_tables=[peripheral_table]
    )

    # ----------------

    # We want 5 additional iterations.
    gaussian_search.n_iter = 5

    # We do not want another burn-in-phase,
    # so we set ratio_iter to 0.
    gaussian_search.ratio_iter = 0.0

    # This widens the hyperparameter space.
    gaussian_search.param_space["feature_learners"][1]["num_features"] = [10, 100]

    # This narrows the hyperparameter space.
    gaussian_search.param_space["predictors"][0]["reg_lambda"] = [0.0, 0.0]

    # This continues the hyperparameter search using the previous iterations as
    # prior knowledge.
    gaussian_search.fit(
        population_table_training=population_table,
        population_table_validation=population_table,
        peripheral_tables=[peripheral_table]
    )

    # ----------------

    all_hyp = hyperopt.list_hyperopts()

    best_pipeline = gaussian_search.best_pipeline
    ```
"""

from .helpers import delete, exists, list_hyperopts
from .hyperopt import GaussianHyperparameterSearch, LatinHypercubeSearch, RandomSearch
from .load_hyperopt import load_hyperopt
from .tuning import tune_feature_learners, tune_predictors

__all__ = (
    "delete",
    "exists",
    "list_hyperopts",
    "load_hyperopt",
    "GaussianHyperparameterSearch",
    "LatinHypercubeSearch",
    "RandomSearch",
    "tune_feature_learners",
    "tune_predictors",
)
