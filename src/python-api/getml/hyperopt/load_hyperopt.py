# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Loads a hyperparameter optimization object from the getML Engine into Python."""

from typing import Union

from getml.pipeline.helpers2 import _make_dummy

from .hyperopt import (
    GaussianHyperparameterSearch,
    LatinHypercubeSearch,
    RandomSearch,
    _get_json_obj,
)


def load_hyperopt(
    name: str,
) -> Union[GaussianHyperparameterSearch, LatinHypercubeSearch, RandomSearch]:
    """Loads a hyperparameter optimization object from the getML Engine into Python.

    Args:
        name:
            The name of the hyperopt to be loaded.

    Returns:
        The hyperopt object.

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
