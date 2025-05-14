# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Checks both the types and values of the `kwargs` and raises an
exception is something is off.
"""

import numbers

import numpy as np

from getml.data.helpers import _is_typed_list
from getml.helpers import _check_parameter_bounds
from getml.pipeline.metrics import _all_metrics

from .burn_in import _all_burn_ins
from .kernels import _all_kernels
from .optimization import _all_optimizations

# -------------------------------------------------------------------------------


def _validate_bounds(subspace, component):
    """Validates the lower and upper bound using a component.

    A component can be a feature_learner, a feature_selector
    or a predictor.
    """
    params = component.__dict__

    # Make sure that the parameter is supported.
    for key in subspace:
        if key not in params:
            raise ValueError(
                component.type + " does not support a parameter called '" + key + "'."
            )

    # Check the lower bounds.
    for key in subspace:
        assert len(subspace[key]) == 2, "Unexpected bound length"
        params[key] = subspace[key][0]

    component.validate(params)

    # Check the upper bounds.
    for key in subspace:
        assert len(subspace[key]) == 2, "Unexpected bound length"
        params[key] = subspace[key][1]

    component.validate(params)


# -------------------------------------------------------------------------------


def _validate_dimension(subspace, original_subspace):
    """Makes sure that no dimensions have been removed from the hyperparameter space."""
    for key in original_subspace:
        if key not in subspace:
            raise ValueError(
                """It appears you have removed a dimension from the
                original param_space: '"""
                + key
                + "'."
            )


# -------------------------------------------------------------------------------


def _validate_components(param_space, original_param_space, components, ltype):
    if ltype in original_param_space:
        if len(param_space[ltype]) != len(original_param_space[ltype]):
            raise ValueError("Number of " + ltype + " must stay the same.")

    if len(param_space[ltype]) != len(components):
        raise ValueError(
            "Number of subspaces for the "
            + ltype
            + "must match the number of "
            + ltype
            + "."
        )

    for i, subspace in enumerate(param_space[ltype]):
        if not isinstance(subspace, dict):
            raise TypeError("subspace must be a dict")

        if len(subspace) == 0:
            raise ValueError("subspace must contain at least one key-value pair")

        for ddimension in subspace:
            if len(subspace[ddimension]) != 2:
                raise ValueError(
                    "In key '"
                    + ddimension
                    + "' of subspace: Must be a numerical array of length 2."
                )

            if not _is_typed_list(subspace[ddimension], numbers.Real):
                raise ValueError(
                    "In key '"
                    + ddimension
                    + "' of 'param_space': All values must be numerical."
                )

            if subspace[ddimension][0] > subspace[ddimension][1]:
                raise ValueError(
                    """In key '"""
                    + ddimension
                    + """' of 'param_space':
                    The lower bound cannot be greater than the upper bound!
                    Syntax 'key: [lower, upper]'"""
                )

        _validate_bounds(subspace, components[i])

        if ltype in original_param_space:
            _validate_dimension(subspace, original_param_space[ltype][i])


# -------------------------------------------------------------------------------


def _validate_param_space(kwargs):
    # ----------------------------------------------------------------

    pipeline = kwargs["pipeline"]

    param_space = kwargs["param_space"]

    original_param_space = kwargs["_original_param_space"]

    # ----------------------------------------------------------------

    if not isinstance(kwargs["param_space"], dict):
        raise TypeError("'param_space' must be a dict")

    if len(kwargs["param_space"]) == 0:
        raise ValueError("'param_space' must contain at least something.")

    # ----------------------------------------------------------------

    if "feature_learns" in param_space:
        _validate_components(
            param_space,
            original_param_space,
            pipeline.feature_learners,
            "feature_learns",
        )
    elif "feature_learns" in original_param_space:
        raise ValueError("'feature_learns' has been removed from the param_space.")

    # ----------------------------------------------------------------

    if "feature_selectors" in param_space:
        _validate_components(
            param_space,
            original_param_space,
            pipeline.feature_selectors,
            "feature_selectors",
        )
    elif "feature_selectors" in original_param_space:
        raise ValueError("'feature_selectors' has been removed from the param_space.")

    # ----------------------------------------------------------------

    if "predictors" in param_space:
        _validate_components(
            param_space, original_param_space, pipeline.predictors, "predictors"
        )
    elif "predictors" in original_param_space:
        raise ValueError("'predictors' has been removed from the param_space.")


# -------------------------------------------------------------------------------


def _validate_hyperopt(supported_params, **kwargs):
    """
    Checks both the types and values of the `kwargs` and raises an
    exception is something is off.

    Examples:
    ```python
    getml.helpers.validation.validate_Hyperopt_kwargs(
        {'n_iter': 10, 'score': 'auc'})
    ```
    Args:
        kwargs (dict): Dictionary containing some of all
            kwargs supported in
            [`RandomSearch`][getml.predictors.RandomSearch],
            [`GaussianHyperparameterSearch`][getml.predictors.GaussianHyperparameterSearch], and
            [`LatinHypercubeSearch`][getml.predictors.LatinHypercubeSearch].

    Note:
        In addition to checking the types and values of the
        `kwargs`, the function also accesses whether:

            - `seed` is only used in a single-threaded context
            - `param_space` is both valid and consistent
            - `session_name` is set
            - `n_iter` matches the requirements of the algorithm
            - `score` is compatible with the loss function provided
                in `model`
    """

    # ----------------------------------------------------------------

    for key in kwargs:
        if key not in supported_params:
            raise ValueError(
                "Parameter '" + key + "' is not supported for " + kwargs["_type"] + "."
            )

    # ----------------------------------------------------------------

    pipeline = kwargs["pipeline"]

    pipeline._validate()

    # ----------------------------------------------------------------

    _validate_param_space(kwargs)

    # ----------------------------------------------------------------

    if kwargs["seed"] is not None and not isinstance(kwargs["seed"], numbers.Real):
        raise TypeError("'seed' must be either None or a real number")

    if kwargs["seed"] is not None:
        _check_parameter_bounds(kwargs["seed"], "seed", [0, np.iinfo(np.uint64).max])

    # ----------------------------------------------------------------

    if not isinstance(kwargs["ratio_iter"], numbers.Real):
        raise TypeError("'ratio_iter' must be a real number")

    _check_parameter_bounds(kwargs["ratio_iter"], "ratio_iter", [0.0, 1.0])

    if not isinstance(kwargs["n_iter"], numbers.Real):
        raise TypeError("'n_iter' must be a real number")

    # ----------------------------------------------------------------

    if kwargs["ratio_iter"] == 1:
        _check_parameter_bounds(kwargs["n_iter"], "n_iter", [1, np.iinfo(np.int32).max])
    else:
        _check_parameter_bounds(kwargs["n_iter"], "n_iter", [4, np.iinfo(np.int32).max])

    # ----------------------------------------------------------------

    if not isinstance(kwargs["optimization_algorithm"], str):
        raise TypeError("'optimization_algorithm' must be a str")

    if kwargs["optimization_algorithm"] not in _all_optimizations:
        raise ValueError(
            "'optimization_algorithm' must be one of the following: "
            + str(_all_optimizations)
            + "."
        )

    # ----------------------------------------------------------------

    if not isinstance(kwargs["optimization_burn_in_algorithm"], str):
        raise TypeError("'optimization_burn_in_algorithm' must be a str")

    if kwargs["optimization_burn_in_algorithm"] not in _all_burn_ins:
        raise ValueError(
            "'optimization_burn_in_algorithm' must one of the following: "
            + str(_all_burn_ins)
            + "."
        )

    # ----------------------------------------------------------------

    if not isinstance(kwargs["optimization_burn_ins"], numbers.Real):
        raise TypeError("'optimization_burn_ins' must be a real number")

    _check_parameter_bounds(
        kwargs["optimization_burn_ins"],
        "optimization_burn_ins",
        [3, np.iinfo(np.int32).max],
    )

    # ----------------------------------------------------------------

    if not isinstance(kwargs["surrogate_burn_in_algorithm"], str):
        raise TypeError("'surrogate_burn_in_algorithm' must be a str")

    if kwargs["surrogate_burn_in_algorithm"] not in _all_burn_ins:
        raise ValueError(
            "'surrogate_burn_in_algorithm' must one of the following: "
            + str(_all_burn_ins)
            + "."
        )

    # ----------------------------------------------------------------

    if not isinstance(kwargs["gaussian_kernel"], str):
        raise TypeError("'gaussian_kernel' must be a str")

    if kwargs["gaussian_kernel"] not in _all_kernels:
        raise ValueError(
            "'gaussian_kernel' must one of the following: " + str(_all_kernels) + "."
        )

    # ----------------------------------------------------------------

    if not isinstance(kwargs["gaussian_optimization_algorithm"], str):
        raise TypeError("'gaussian_optimization_algorithm' must be a str")

    if kwargs["gaussian_optimization_algorithm"] not in _all_optimizations:
        raise ValueError(
            "'gaussian_optimization_algorithm' must be one of the following: "
            + str(_all_optimizations)
            + "."
        )

    # ----------------------------------------------------------------

    if not isinstance(kwargs["gaussian_optimization_burn_in_algorithm"], str):
        raise TypeError("'gaussian_optimization_burn_in_algorithm' must be a str")

    if kwargs["gaussian_optimization_burn_in_algorithm"] not in _all_burn_ins:
        raise ValueError(
            "'gaussian_optimization_burn_in_algorithm' must be one of the following: "
            + str(_all_burn_ins)
            + "."
        )

    # ----------------------------------------------------------------

    if not isinstance(kwargs["gaussian_optimization_burn_ins"], numbers.Real):
        raise TypeError("'gaussian_optimization_burn_ins' must be a real number")

    _check_parameter_bounds(
        kwargs["gaussian_optimization_burn_ins"],
        "gaussian_optimization_burn_ins",
        [3, np.iinfo(np.int32).max],
    )

    # ----------------------------------------------------------------

    if not isinstance(kwargs["_score"], str):
        raise TypeError("'score' must be a str")

    if kwargs["_score"] not in _all_metrics:
        raise ValueError(
            "'score' must be one of the following: " + str(_all_metrics) + "."
        )

    # ----------------------------------------------------------------
