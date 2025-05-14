# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Tuning routines simplify the hyperparameter optimizations and
are the recommended way of tuning hyperparameters.
"""

import copy
import numbers
import time
from typing import Any, Dict, Optional

from rich import print

import getml.communication as comm
import getml.pipeline
from getml.data import Container, StarSchema, TimeSeries
from getml.pipeline import Pipeline, metrics
from getml.pipeline.helpers import _print_time_taken, _transform_peripheral

# -----------------------------------------------------------------------------


def _infer_score(pipeline):
    if pipeline.is_classification:
        return getml.pipeline.metrics.auc
    return getml.pipeline.metrics.rmse


# -----------------------------------------------------------------------------


def _make_final_pipeline(
    pipeline: Pipeline,
    tuned_feature_learners,
    tuned_predictors,
    container,
    train,
    validation,
) -> Pipeline:
    print("Building final pipeline...")

    final_pipeline = copy.deepcopy(pipeline)

    final_pipeline.feature_learners = tuned_feature_learners
    final_pipeline.predictors = tuned_predictors

    final_pipeline.fit(
        population_table=container[train], validation_table=container[validation]
    )

    final_pipeline.score(container[validation])

    return final_pipeline


# -----------------------------------------------------------------------------


def _tune(
    what,
    pipeline,
    container,
    train,
    validation,
    n_iter=111,
    score=metrics.rmse,
    num_threads=0,
) -> Pipeline:
    """
    Internal base tuning function that is called by other tuning functions.
    """

    pipeline = copy.deepcopy(pipeline)

    if isinstance(container, (StarSchema, TimeSeries)):
        container = container.container

    pipeline.check(container[train])

    if not isinstance(container, Container):
        raise TypeError(
            "'container' must be a `~getml.data.Container`, "
            + "a `~getml.data.StarSchema` or a `~getml.data.TimeSeries`"
        )

    if not isinstance(train, str):
        raise TypeError("""'train' must be a string""")

    if not isinstance(validation, str):
        raise TypeError("""'validation' must be a string""")

    if not isinstance(n_iter, numbers.Real):
        raise TypeError("""'n_iter' must be a real number""")

    if not isinstance(num_threads, numbers.Real):
        raise TypeError("""'num_threads' must be a real number""")

    population_table_training = container[train].population

    population_table_validation = container[validation].population

    peripheral_tables = _transform_peripheral(
        container[train].peripheral, pipeline.peripheral
    )

    cmd: Dict[str, Any] = {}

    cmd["name_"] = ""
    cmd["type_"] = "Hyperopt.tune"

    cmd["n_iter_"] = n_iter
    cmd["num_threads_"] = num_threads
    cmd["pipeline_"] = pipeline._getml_deserialize()
    cmd["score_"] = score
    cmd["what_"] = what

    cmd["population_training_df_"] = population_table_training._getml_deserialize()
    cmd["population_validation_df_"] = population_table_validation._getml_deserialize()
    cmd["peripheral_dfs_"] = [elem._getml_deserialize() for elem in peripheral_tables]

    with comm.send_and_get_socket(cmd) as sock:
        begin = time.monotonic()
        msg = comm.log(sock, extra={"cmd": cmd})
        end = time.monotonic()
        if msg != "Success!":
            comm.handle_engine_exception(msg)
        _print_time_taken(begin, end, "Time taken: ")
        pipeline_name = comm.recv_string(sock)

    return getml.pipeline.load(pipeline_name)


# -----------------------------------------------------------------------------


def _tune_feature_learner(
    feature_learner,
    pipeline,
    container,
    train,
    validation,
    n_iter,
    score,
    num_threads,
) -> Pipeline:
    if feature_learner.type not in [
        "Fastboost",
        "FastProp",
        "Multirel",
        "Relboost",
        "RelMT",
    ]:
        raise ValueError("Unknown feature learner: " + feature_learner.type + "!")

    return _tune(
        feature_learner.type,
        pipeline,
        container,
        train,
        validation,
        n_iter,
        score,
        num_threads,
    )


# -----------------------------------------------------------------------------


def _tune_predictor(
    predictor,
    pipeline,
    container,
    train,
    validation,
    n_iter,
    score,
    num_threads,
) -> Pipeline:
    if "ScaleGBM" in predictor.type:
        return _tune(
            "ScaleGBM",
            pipeline,
            container,
            train,
            validation,
            n_iter,
            score,
            num_threads,
        )

    if "XGBoost" in predictor.type:
        return _tune(
            "XGBoost",
            pipeline,
            container,
            train,
            validation,
            n_iter,
            score,
            num_threads,
        )

    if "Regression" in predictor.type:
        return _tune(
            "Linear",
            pipeline,
            container,
            train,
            validation,
            n_iter,
            score,
            num_threads,
        )

    raise ValueError("Unknown predictor: '" + predictor.type + "'!")


# -----------------------------------------------------------------------------


def tune_feature_learners(
    pipeline: Pipeline,
    container: Container,
    train: str = "train",
    validation: str = "validation",
    n_iter: int = 0,
    score: Optional[str] = None,
    num_threads: int = 0,
) -> Pipeline:
    """
    A high-level interface for optimizing the feature learners of a
    [`Pipeline`][getml.pipeline.Pipeline].

    Efficiently optimizes the hyperparameters for the set of feature learners
    (from [`feature_learning`][getml.feature_learning]) of a given pipeline by breaking each
    feature learner's hyperparameter space down into carefully curated
    subspaces: `hyperopt_tuning_subspaces` and optimizing the hyperparameters for
    each subspace in a sequential multi-step process.  For further details about
    the actual recipes behind the tuning routines refer
    to tuning routines: `hyperopt_tuning`.

    Args:
        pipeline:
            Base pipeline used to derive all models fitted and scored
            during the hyperparameter optimization. It defines the data
            schema and any hyperparameters that are not optimized.

        container:
            The data container used for the hyperparameter tuning.

        train:
            The name of the subset in 'container' used for training.

        validation:
            The name of the subset in 'container' used for validation.

        n_iter:
            The number of iterations.

        score:
            The score to optimize. Must be from
            [`metrics`][getml.pipeline.metrics].

        num_threads:
            The number of parallel threads to use. If set to 0,
            the number of threads will be inferred.

    Returns:
        Pipeline containing tuned versions of the feature learners.

    ??? example
        We assume that you have already set up your
        [`Pipeline`][getml.Pipeline] and
        [`Container`][getml.data.Container].

        ```python
        tuned_pipeline = getml.hyperopt.tune_predictors(
            pipeline=base_pipeline,
            container=container)
        ```
    """

    if not isinstance(pipeline, getml.pipeline.Pipeline):
        raise TypeError("'pipeline' must be a pipeline!")

    pipeline._validate()

    if not score:
        score = _infer_score(pipeline)

    tuned_feature_learners = []

    for feature_learner in pipeline.feature_learners:
        tuned_pipeline = _tune_feature_learner(
            feature_learner=feature_learner,
            pipeline=pipeline,
            container=container,
            train=train,
            validation=validation,
            n_iter=n_iter,
            score=score,
            num_threads=num_threads,
        )

        assert len(tuned_pipeline.feature_learners) == 1, (
            "Expected exactly one feature learner, got "
            + str(len(tuned_pipeline.feature_learners))
        )

        tuned_feature_learners.append(tuned_pipeline.feature_learners[0])

    return _make_final_pipeline(
        pipeline,
        tuned_feature_learners,
        copy.deepcopy(pipeline.predictors),
        container,
        train,
        validation,
    )


# -----------------------------------------------------------------------------


def tune_predictors(
    pipeline: Pipeline,
    container: Container,
    train: str = "train",
    validation: str = "validation",
    n_iter: int = 0,
    score: Optional[str] = None,
    num_threads: int = 0,
) -> Pipeline:
    """
    A high-level interface for optimizing the predictors of a
    [`Pipeline`][getml.Pipeline].

    Efficiently optimizes the hyperparameters for the set of predictors (from
    [`getml.predictors`][getml.predictors]) of a given pipeline by breaking each
    predictor's
    hyperparameter space down into carefully curated
    subspaces: `hyperopt_tuning_subspaces` and optimizing the hyperparameters for
    each subspace in a sequential multi-step process.  For further details about
    the actual recipes behind the tuning routines refer to
    tuning routines: `hyperopt_tuning`.

    Args:
        pipeline:
            Base pipeline used to derive all models fitted and scored
            during the hyperparameter optimization. It defines the data
            schema and any hyperparameters that are not optimized.

        container:
            The data container used for the hyperparameter tuning.

        train:
            The name of the subset in 'container' used for training.

        validation:
            The name of the subset in 'container' used for validation.

        n_iter:
            The number of iterations.

        score:
            The score to optimize. Must be from
            [`metrics`][getml.pipeline.metrics].

        num_threads:
            The number of parallel threads to use. If set to 0,
            the number of threads will be inferred.

    ??? example
        We assume that you have already set up your
        [`Pipeline`][getml.Pipeline] and
        [`Container`][getml.data.Container].

        ```python
        tuned_pipeline = getml.hyperopt.tune_predictors(
            pipeline=base_pipeline,
            container=container)
        ```

    Returns:
        Pipeline containing tuned predictors.

    """

    if not isinstance(pipeline, getml.pipeline.Pipeline):
        raise TypeError("'pipeline' must be a pipeline!")

    pipeline._validate()

    if not score:
        score = _infer_score(pipeline)

    tuned_predictors = []

    for predictor in pipeline.predictors:
        tuned_pipeline = _tune_predictor(
            predictor=predictor,
            pipeline=pipeline,
            container=container,
            train=train,
            validation=validation,
            n_iter=n_iter,
            score=score,
            num_threads=num_threads,
        )

        assert len(tuned_pipeline.predictors) == 1, (
            "Expected exactly one predictor, got " + str(len(tuned_pipeline.predictors))
        )

        tuned_predictors.append(tuned_pipeline.predictors[0])

    return _make_final_pipeline(
        pipeline,
        copy.deepcopy(pipeline.feature_learners),
        tuned_predictors,
        container,
        train,
        validation,
    )
