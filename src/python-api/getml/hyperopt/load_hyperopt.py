# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Loads a hyperparameter optimization object from the getML engine into Python."""

from getml.data import Placeholder
from getml.pipeline import Pipeline
from getml.predictors import LinearRegression
from getml.pipeline.helpers2 import _make_dummy


from .hyperopt import (
    GaussianHyperparameterSearch,
    LatinHypercubeSearch,
    RandomSearch,
    _get_json_obj,
)


def load_hyperopt(name):
    """Loads a hyperparameter optimization object from the getML engine into Python.

    Args:
        name (str):
            The name of the hyperopt to be loaded.

    Returns:
        A :class:`~getml.hyperopt.GaussianHyperparameterSearch` that is a handler
        for the pipeline signified by name.
    
    Note:
        Not supported in the getML community edition.
    """
    # This will be overwritten by .refresh(...) anyway
    dummy_pipeline = _make_dummy("123456")

    dummy_param_space = {"predictors": [{"reg_lambda": [0.0, 1.0]}]}

    json_obj = _get_json_obj(name)

    if json_obj["type_"] == "GaussianHyperparameterSearch":
        return GaussianHyperparameterSearch(
            param_space=dummy_param_space, pipeline=dummy_pipeline
        )._parse_json_obj(json_obj)

    if json_obj["type_"] == "LatinHypercubeSearch":
        return LatinHypercubeSearch(
            param_space=dummy_param_space, pipeline=dummy_pipeline
        )._parse_json_obj(json_obj)

    if json_obj["type_"] == "RandomSearch":
        return RandomSearch(
            param_space=dummy_param_space, pipeline=dummy_pipeline
        )._parse_json_obj(json_obj)

    raise ValueError("Unknown type: '" + json_obj["type_"] + "'!")
