# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains helper functions for validating the feature learning algorithms.
"""

from __future__ import annotations

import numbers
from typing import Any, cast

import numpy as np

from getml import feature_learning
from getml.data.helpers import _is_non_empty_typed_list, _is_typed_list
from getml.helpers import _check_parameter_bounds, _is_iterable_not_str_of_type
from getml.log import MIGHT_TAKE_LONG, logger

from .aggregations.sets import _all_aggregations, _multirel_subset

# --------------------------------------------------------------------


class Validator:
    def __init__(self, name: str) -> None:
        self.name = name

    def __set__(self, instance: feature_learning._FeatureLearner, value: Any) -> None:
        # dont call validate upon initialization
        if self.name in instance.__dict__:
            instance.validate({self.name: value})
        instance.__dict__[self.name] = value


# --------------------------------------------------------------------


def _validate_fastprop_parameters(**kwargs: Any) -> None:
    aggregation = kwargs["aggregation"]
    delta_t = kwargs["delta_t"]
    loss_function = kwargs["loss_function"]
    max_lag = kwargs["max_lag"]
    min_df = kwargs["min_df"]
    n_most_frequent = kwargs["n_most_frequent"]
    num_features = kwargs["num_features"]
    num_threads = kwargs["num_threads"]
    sampling_factor = kwargs["sampling_factor"]
    silent = kwargs["silent"]
    vocab_size = kwargs["vocab_size"]

    if not _is_iterable_not_str_of_type(aggregation, str):
        raise TypeError(
            """
            'aggregation' must be a non-empty
            non-string iterable of str found in getml.feature_learning.aggregations
            """
        )

    if not isinstance(delta_t, numbers.Real):
        raise TypeError("'delta_t' must be a real number")

    if not isinstance(loss_function, str) or loss_function is None:
        raise TypeError("'loss_function' must be a str")

    if not isinstance(max_lag, int):
        raise TypeError("'max_lag' must be an integer")

    if not isinstance(min_df, numbers.Real):
        raise TypeError("'min_df' must be a real number")

    if not isinstance(num_features, numbers.Real):
        raise TypeError("'num_features' must be a real number")

    if not isinstance(n_most_frequent, numbers.Real):
        raise TypeError("'n_most_frequent' must be a real number")

    if not isinstance(num_threads, numbers.Real):
        raise TypeError("'num_threads' must be a real number")

    if not isinstance(sampling_factor, numbers.Real):
        raise TypeError("'sampling_factor' must be a real number")

    if not isinstance(silent, bool):
        raise TypeError("'silent' must be of type bool")

    if not isinstance(vocab_size, numbers.Real):
        raise TypeError("'vocab_size' must be a real number")

    if not all([aa in _all_aggregations for aa in aggregation]):
        raise ValueError(
            f"""
            'aggregation' must be a list of the composed of the following
               aggregations defined in getml.feature_learning.aggregations:
            {_all_aggregations}.
            """
        )

    _check_parameter_bounds(delta_t, "delta_t", [0, np.finfo(np.float64).max])

    _check_parameter_bounds(max_lag, "max_lag", [0, np.iinfo(np.int32).max])

    _check_parameter_bounds(min_df, "min_df", [0, np.iinfo(np.int32).max])

    _check_parameter_bounds(
        n_most_frequent, "n_most_frequent", [0, np.iinfo(np.int32).max]
    )

    _check_parameter_bounds(num_features, "num_features", [1, np.iinfo(np.int32).max])

    _check_parameter_bounds(num_threads, "num_threads", [0, np.iinfo(np.int32).max])

    _check_parameter_bounds(
        sampling_factor, "sampling_factor", [0, np.finfo(np.float64).max]
    )

    _check_parameter_bounds(vocab_size, "vocab_size", [0, np.finfo(np.float64).max])

    if (cast(float, delta_t) > 0.0) ^ (max_lag > 0):
        raise ValueError(
            """
            If you pass a non-zero value to delta_t, you must also
            pass a non-zero
            value to max_lag and vice-versa.
            """
        )


# --------------------------------------------------------------------


def _validate_fastboost_parameters(**kwargs: Any) -> None:
    """
    Checks both the types and values of the `parameters` belonging to
    [`Fastboost`][getml.feature_learning.Fastboost] and raises an exception if
    something is off.
    """

    gamma = kwargs["gamma"]
    loss_function = kwargs["loss_function"]
    max_depth = kwargs["max_depth"]
    min_child_weights = kwargs["min_child_weights"]
    num_features = kwargs["num_features"]
    num_threads = kwargs["num_threads"]
    reg_lambda = kwargs["reg_lambda"]
    seed = kwargs["seed"]
    shrinkage = kwargs["shrinkage"]
    subsample = kwargs["subsample"]

    if not isinstance(gamma, numbers.Real):
        raise TypeError("'gamma' must be a real number")

    if not isinstance(loss_function, str) or loss_function is None:
        raise TypeError("'loss_function' must be a str")

    if not isinstance(max_depth, numbers.Real):
        raise TypeError("'max_depth' must be a real number")

    if not isinstance(min_child_weights, numbers.Real):
        raise TypeError("'min_child_weights' must be a real number")

    if not isinstance(num_features, numbers.Real):
        raise TypeError("'num_features' must be a real number")

    if not isinstance(num_threads, numbers.Real):
        raise TypeError("'num_threads' must be a real number")

    if not isinstance(reg_lambda, numbers.Real):
        raise TypeError("'reg_lambda' must be a real number")

    if not isinstance(seed, numbers.Real):
        raise TypeError("'seed' must be a real number or None")

    if not isinstance(shrinkage, numbers.Real):
        raise TypeError("'shrinkage' must be a real number")

    if not isinstance(subsample, numbers.Real):
        raise TypeError("'subsample' must be a real number")

    _check_parameter_bounds(gamma, "gamma", [0.0, np.finfo(np.float64).max])

    _check_parameter_bounds(max_depth, "max_depth", [0, np.iinfo(np.int32).max])

    _check_parameter_bounds(
        min_child_weights, "min_child_weights", [0.0, np.finfo(np.float64).max]
    )

    _check_parameter_bounds(num_features, "num_features", [1, np.iinfo(np.int32).max])

    _check_parameter_bounds(num_threads, "num_threads", [0, np.iinfo(np.int32).max])

    _check_parameter_bounds(reg_lambda, "reg_lambda", [0.0, np.finfo(np.float64).max])

    _check_parameter_bounds(seed, "seed", [0.0, np.iinfo(np.uint64).max])

    _check_parameter_bounds(shrinkage, "shrinkage", [0.0, 1.0])

    _check_parameter_bounds(subsample, "subsample", [0.0, np.finfo(np.float64).max])


# --------------------------------------------------------------------


def _validate_multirel_parameters(**kwargs: Any) -> None:
    # ----------------------------------------------------------------

    aggregation = kwargs["aggregation"]
    allow_sets = kwargs["allow_sets"]
    delta_t = kwargs["delta_t"]
    grid_factor = kwargs["grid_factor"]
    loss_function = kwargs["loss_function"]
    max_length = kwargs["max_length"]
    min_df = kwargs["min_df"]
    min_num_samples = kwargs["min_num_samples"]
    num_features = kwargs["num_features"]
    num_subfeatures = kwargs["num_subfeatures"]
    num_threads = kwargs["num_threads"]
    propositionalization = kwargs["propositionalization"]
    regularization = kwargs["regularization"]
    round_robin = kwargs["round_robin"]
    sampling_factor = kwargs["sampling_factor"]
    seed = kwargs["seed"]
    share_aggregations = kwargs["share_aggregations"]
    share_conditions = kwargs["share_conditions"]
    shrinkage = kwargs["shrinkage"]
    vocab_size = kwargs["vocab_size"]

    # ----------------------------------------------------------------

    if not _is_iterable_not_str_of_type(aggregation, str):
        raise TypeError(
            """
            'aggregation' must be a non-empty
            non-string iterable of str found in getml.feature_learning.aggregations
            """
        )
    if not isinstance(allow_sets, bool):
        raise TypeError("'allow_sets' must be of type bool")

    if not isinstance(delta_t, numbers.Real):
        raise TypeError("'delta_t' must be a real number")

    if not isinstance(grid_factor, numbers.Real):
        raise TypeError("'grid_factor' must be a real number")

    if not isinstance(loss_function, str) or loss_function is None:
        raise TypeError("'loss_function' must be a str")

    if not isinstance(max_length, numbers.Real):
        raise TypeError("'max_length' must be a real number")

    if not isinstance(min_df, numbers.Real):
        raise TypeError("'min_df' must be a real number")

    if not isinstance(min_num_samples, numbers.Real):
        raise TypeError("'min_num_samples' must be a real number")

    if not isinstance(num_features, numbers.Real):
        raise TypeError("'num_features' must be a real number")

    if not isinstance(num_subfeatures, numbers.Real):
        raise TypeError("'num_subfeatures' must be a real number")

    if not isinstance(num_threads, numbers.Real):
        raise TypeError("'num_threads' must be a real number")

    if type(propositionalization).__name__ != "FastProp":
        raise TypeError("'propositionalization' must be a FastProp")

    if not isinstance(regularization, numbers.Real):
        raise TypeError("'regularization' must be a real number")

    if not isinstance(round_robin, bool):
        raise TypeError("'round_robin' must be of type bool")

    if not isinstance(sampling_factor, numbers.Real):
        raise TypeError("'sampling_factor' must be a real number")

    if not isinstance(seed, numbers.Real):
        raise TypeError("'seed' must be a real number or None")

    if not isinstance(share_aggregations, numbers.Real):
        raise TypeError("'share_aggregations' must be a real number")

    if not isinstance(share_conditions, numbers.Real):
        raise TypeError("'share_conditions' must be a real number")

    if not isinstance(shrinkage, numbers.Real):
        raise TypeError("'shrinkage' must be a real number")

    if not isinstance(vocab_size, numbers.Real):
        raise TypeError("'vocab_size' must be a real number")

    _check_parameter_bounds(vocab_size, "vocab_size", [0, np.finfo(np.float64).max])

    # ----------------------------------------------------------------

    if not all([aa in _multirel_subset for aa in aggregation]):
        raise ValueError(
            f"""
            'aggregation' must be a list of the composed of the following
            aggregations defined in getml.feature_learning.aggregations:
            {_multirel_subset}.
            """
        )

    _check_parameter_bounds(delta_t, "delta_t", [0.0, np.finfo(np.float64).max])

    _check_parameter_bounds(
        grid_factor,
        "grid_factor",
        [
            np.finfo(np.float64).resolution,  # pylint: disable=E1101
            np.finfo(np.float64).max,
        ],
    )

    _check_parameter_bounds(max_length, "max_length", [0, np.iinfo(np.int32).max])

    _check_parameter_bounds(min_df, "min_df", [0, np.iinfo(np.int32).max])

    _check_parameter_bounds(
        min_num_samples, "min_num_samples", [1, np.iinfo(np.int32).max]
    )

    _check_parameter_bounds(num_features, "num_features", [1, np.iinfo(np.int32).max])

    _check_parameter_bounds(
        num_subfeatures, "num_subfeatures", [1, np.iinfo(np.int32).max]
    )

    _check_parameter_bounds(num_threads, "num_threads", [0, np.iinfo(np.int32).max])

    _check_parameter_bounds(regularization, "regularization", [0.0, 1.0])

    _check_parameter_bounds(
        sampling_factor, "sampling_factor", [0.0, np.finfo(np.float64).max]
    )

    _check_parameter_bounds(seed, "seed", [0.0, np.iinfo(np.uint64).max])

    _check_parameter_bounds(share_aggregations, "share_aggregations", [0.0, 1.0])

    _check_parameter_bounds(share_conditions, "share_conditions", [0.0, 1.0])

    _check_parameter_bounds(shrinkage, "shrinkage", [0.0, 1.0])

    # ----------------------------------------------------------------

    if cast(int, num_subfeatures) > 10:
        logger.info(
            f"""
            {MIGHT_TAKE_LONG}
            You have set num_subfeatures to {num_subfeatures}.
            The multirel algorithm does not scale well to many columns.
            You should consider using Relboost instead.
            """
        )


# --------------------------------------------------------------------


def _validate_relboost_parameters(**kwargs: Any) -> None:
    """
    Checks both the types and values of the `parameters` belonging to
    [`Relboost`][getml.feature_learning.Relboost] and raises an exception if
    something is off.
    """

    # ----------------------------------------------------------------

    allow_avg = kwargs.get("allow_avg", False)
    allow_null_weights = kwargs.get("allow_null_weights", False)

    # ----------------------------------------------------------------

    delta_t = kwargs["delta_t"]
    gamma = kwargs["gamma"]
    loss_function = kwargs["loss_function"]
    max_depth = kwargs["max_depth"]
    min_df = kwargs["min_df"]
    min_num_samples = kwargs["min_num_samples"]
    num_features = kwargs["num_features"]
    num_subfeatures = kwargs["num_subfeatures"]
    num_threads = kwargs["num_threads"]
    propositionalization = kwargs["propositionalization"]
    reg_lambda = kwargs["reg_lambda"]
    sampling_factor = kwargs["sampling_factor"]
    seed = kwargs["seed"]
    shrinkage = kwargs["shrinkage"]
    vocab_size = kwargs["vocab_size"]

    # ----------------------------------------------------------------

    if not isinstance(allow_avg, bool):
        raise TypeError("'allow_avg' must be of type bool")

    if not isinstance(allow_null_weights, bool):
        raise TypeError("'allow_null_weights' must be of type bool")

    if not isinstance(delta_t, numbers.Real):
        raise TypeError("'delta_t' must be a real number")

    if not isinstance(gamma, numbers.Real):
        raise TypeError("'gamma' must be a real number")

    if not isinstance(loss_function, str) or loss_function is None:
        raise TypeError("'loss_function' must be a str")

    if not isinstance(max_depth, numbers.Real):
        raise TypeError("'max_depth' must be a real number")

    if not isinstance(min_df, numbers.Real):
        raise TypeError("'min_df' must be a real number")

    if not isinstance(min_num_samples, numbers.Real):
        raise TypeError("'min_num_samples' must be a real number")

    if not isinstance(num_features, numbers.Real):
        raise TypeError("'num_features' must be a real number")

    if not isinstance(num_subfeatures, numbers.Real):
        raise TypeError("'num_subfeatures' must be a real number")

    if not isinstance(num_threads, numbers.Real):
        raise TypeError("'num_threads' must be a real number")

    if type(propositionalization).__name__ != "FastProp":
        raise TypeError("'propositionalization' must be a FastProp")

    if not isinstance(reg_lambda, numbers.Real):
        raise TypeError("'reg_lambda' must be a real number")

    if not isinstance(sampling_factor, numbers.Real):
        raise TypeError("'sampling_factor' must be a real number")

    if not isinstance(seed, numbers.Real):
        raise TypeError("'seed' must be a real number or None")

    if not isinstance(shrinkage, numbers.Real):
        raise TypeError("'shrinkage' must be a real number")

    if not isinstance(vocab_size, numbers.Real):
        raise TypeError("'vocab_size' must be a real number")

    # ----------------------------------------------------------------

    _check_parameter_bounds(delta_t, "delta_t", [0.0, np.finfo(np.float64).max])

    _check_parameter_bounds(gamma, "gamma", [0.0, np.finfo(np.float64).max])

    _check_parameter_bounds(max_depth, "max_depth", [0, np.iinfo(np.int32).max])

    _check_parameter_bounds(min_df, "min_df", [0, np.iinfo(np.int32).max])

    _check_parameter_bounds(
        min_num_samples, "min_num_samples", [1, np.iinfo(np.int32).max]
    )

    _check_parameter_bounds(num_features, "num_features", [1, np.iinfo(np.int32).max])

    _check_parameter_bounds(
        num_subfeatures, "num_subfeatures", [1, np.iinfo(np.int32).max]
    )

    _check_parameter_bounds(num_threads, "num_threads", [0, np.iinfo(np.int32).max])

    _check_parameter_bounds(reg_lambda, "reg_lambda", [0.0, np.finfo(np.float64).max])

    _check_parameter_bounds(
        sampling_factor, "sampling_factor", [0.0, np.finfo(np.float64).max]
    )

    _check_parameter_bounds(seed, "seed", [0.0, np.iinfo(np.uint64).max])

    _check_parameter_bounds(shrinkage, "shrinkage", [0.0, 1.0])

    _check_parameter_bounds(vocab_size, "vocab_size", [0, np.finfo(np.float64).max])


# --------------------------------------------------------------------


def _validate_time_series_parameters(**kwargs: Any) -> None:
    """
    Validates the parameters that are specific to the time series
    models and raises and exception if something is off.
    """
    # ----------------------------------------------------------------

    horizon = kwargs["horizon"]
    memory = kwargs["memory"]
    self_join_keys = kwargs["self_join_keys"]
    ts_name = kwargs["ts_name"]
    lagged_targets = kwargs["lagged_targets"]

    # ----------------------------------------------------------------

    if not isinstance(horizon, numbers.Real):
        raise TypeError("'horizon' must be a real number")

    if not isinstance(memory, numbers.Real):
        raise TypeError("'memory' must be a real number")

    if not _is_typed_list(self_join_keys, str):
        raise TypeError("'self_join_keys' must be list of str.")

    if not isinstance(ts_name, str):
        raise TypeError("'ts_name' must be a str")

    if not isinstance(lagged_targets, bool):
        raise TypeError("'lagged_targets' must be a bool")

    # ----------------------------------------------------------------

    _check_parameter_bounds(horizon, "horizon", [0.0, np.finfo(np.float64).max])

    _check_parameter_bounds(memory, "memory", [0.0, np.finfo(np.float64).max])

    if horizon == 0.0 and lagged_targets:
        raise ValueError(
            """
            If your horizon is 0.0, then you cannot use
            lagged_targets. This is a data leak.
            """
        )

    # --------------------------------------------------------------------
