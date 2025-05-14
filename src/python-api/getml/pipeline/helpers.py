# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Collection of helper functions not intended to be used by the end-user.
"""

from __future__ import annotations

import builtins
import random
import string
from copy import deepcopy
from datetime import timedelta
from typing import Any, Dict, List, Optional, Sequence, Union

import numpy as np
from rich import print

from getml.data import DataFrame, Placeholder, Roles, View
from getml.data.helpers import _is_typed_list, _remove_trailing_underscores
from getml.feature_learning import (
    Fastboost,
    FastProp,
    Multirel,
    Relboost,
    RelMT,
)
from getml.feature_learning.loss_functions import _all_loss_functions
from getml.predictors import (
    LinearRegression,
    LogisticRegression,
    ScaleGBMClassifier,
    ScaleGBMRegressor,
    XGBoostClassifier,
    XGBoostRegressor,
)
from getml.preprocessors import (
    CategoryTrimmer,
    EmailDomain,
    Imputation,
    Mapping,
    Seasonal,
    Substring,
    TextFieldSplitter,
)

from .metadata import Metadata

POPULATION = "[POPULATION]"
"""
Population marker - the names of the population and peripheral
tables may overlap, so markers are necessary.
"""

PERIPHERAL = "[PERIPHERAL]"
"""
Peripheral marker - the names of the population and peripheral
tables may overlap, so markers are necessary.
"""


def _attach_empty(my_list: List, max_length: int, empty_val: Any) -> List:
    assert len(my_list) <= max_length, "length exceeds max_length!"
    diff = max_length - len(my_list)
    return my_list + [empty_val] * diff


# --------------------------------------------------------------------


def _check_df_types(
    population_table: Union[DataFrame, View],
    peripheral_tables: Sequence[Union[DataFrame, View]],
    validation_table: Optional[Union[DataFrame, View]] = None,
) -> None:
    if not isinstance(population_table, (DataFrame, View)):
        raise TypeError(
            "'population_table' must be a getml.DataFrame or a "
            + "getml.data.View, got "
            + type(population_table).__name__
        )

    if not _is_typed_list(peripheral_tables, (DataFrame, View)):
        raise TypeError(
            """'peripheral_tables' must be a getml.DataFrame, a getml.data.View,
                          or a list of those"""
        )

    if validation_table is not None and not isinstance(
        validation_table, (DataFrame, View)
    ):
        raise TypeError(
            "'validation_table' must be a getml.DataFrame or a getml.data.View."
        )

    if isinstance(population_table, View):
        population_table.check()

    for table in peripheral_tables:
        if isinstance(table, View):
            table.check()

    if isinstance(validation_table, View):
        validation_table.check()


# --------------------------------------------------------------------


def _drop(
    base: Union[DataFrame, View],
    keep_columns: List[Dict[str, str]],
    table: Optional[str],
    marker: str,
) -> View:
    assert isinstance(base, (DataFrame, View)), "Wrong type"
    assert isinstance(table, str) or table is None, "Wrong type"
    assert marker in (POPULATION, PERIPHERAL), "Unknown marker"

    keep_colnames = [
        desc["name_"]
        for desc in keep_columns
        if (desc["table_"] == table and desc["marker_"] == marker)
        or (marker == POPULATION and desc["marker_"] == POPULATION)
    ]

    base.refresh()

    drop = (
        [col for col in base._categorical_names if col not in keep_colnames]
        + [col for col in base._numerical_names if col not in keep_colnames]
        + [col for col in base._text_names if col not in keep_colnames]
        + base._unused_names
    )

    return base.drop(drop)


# --------------------------------------------------------------------


def _edit_table_name(table_name: str) -> str:
    for char in '"`[]':
        table_name = table_name.split(char)[0]
    return table_name


# --------------------------------------------------------------------


def _edit_windows_filename(file_name: str) -> str:
    return "".join([c for c in file_name if c not in '<>:"\\/|?*'])


# --------------------------------------------------------------------


def _extract_df_by_name(
    df_dict: Dict[str, Union[DataFrame, View]], name: str
) -> Union[DataFrame, View]:
    """
    Extracts a data frame signified by name from df_dict.
    """

    if name not in df_dict:
        raise ValueError(
            """The dictionary you passed contains no key
               called '"""
            + name
            + "'."
        )

    if not isinstance(df_dict[name], (DataFrame, View)):
        raise TypeError(
            "'"
            + name
            + "' must be a getml.DataFrame or a getml.data.View, got "
            + type(df_dict[name]).__name__
            + "."
        )

    return df_dict[name]


# --------------------------------------------------------------------


def _extract_dfs(
    df_dict: Dict[str, Union[DataFrame, View]], placeholders: List[Placeholder]
) -> List[Union[DataFrame, View]]:
    """
    Transforms df_dict into a list of data frames
    corresponding to the placeholders.
    """
    if not _is_typed_list(placeholders, Placeholder):
        raise TypeError("'placeholders' must be a list of Placeholders")
    return [_extract_df_by_name(df_dict, elem.name) for elem in placeholders]


# --------------------------------------------------------------------


def _infer_peripheral(population: Placeholder) -> List[Placeholder]:
    def _get_names(population):
        return [j.right.name for j in population.joins] + [
            name for j in population.joins for name in _get_names(j.right)
        ]

    names = _get_names(population)
    names = np.unique(names)
    np.sort(names)
    return [Placeholder(name) for name in names.tolist()]


# --------------------------------------------------------------------


def _handle_loss_function(
    feature_learner: Union[Fastboost, FastProp, Multirel, Relboost, RelMT],  # type: ignore
    loss_function: Optional[str],
) -> Union[Fastboost, FastProp, Multirel, Relboost, RelMT]:  # type: ignore
    if not isinstance(loss_function, str) and loss_function is not None:
        raise TypeError("'loss_function' must be str or None.")

    if loss_function is not None and loss_function not in _all_loss_functions:
        raise ValueError(
            "'loss_function' must be from "
            + "getml.feature_learning.loss_functions, "
            + "meaning that it is one of the following: "
            + str(_all_loss_functions)
            + "."
        )

    if loss_function is None and feature_learner.loss_function is None:
        raise ValueError(
            "You must set a loss function either on the feature learner "
            "or the pipeline"
        )

    feature_learner = deepcopy(feature_learner)

    feature_learner.loss_function = feature_learner.loss_function or loss_function

    if isinstance(feature_learner, (Multirel, Relboost, RelMT)):
        feature_learner.propositionalization.loss_function = (
            feature_learner.propositionalization.loss_function
            or feature_learner.loss_function
        )

    return feature_learner


# --------------------------------------------------------------------


def _make_id() -> str:
    letters = string.ascii_letters + string.digits
    return "".join([random.choice(letters) for _ in range(6)])


# --------------------------------------------------------------------


def _parse_fe(
    dict_: Dict[str, Any],
) -> Union[FastProp, Fastboost, Multirel, Relboost, RelMT]:
    kwargs = _remove_trailing_underscores(dict_)

    fe_type = kwargs["type"]

    del kwargs["type"]

    if "propositionalization" in kwargs:
        kwargs["propositionalization"] = _parse_fe(kwargs["propositionalization"])

    if fe_type == "Fastboost":
        return Fastboost(**kwargs)

    if fe_type == "FastProp":
        return FastProp(**kwargs)

    if fe_type == "Multirel":
        return Multirel(**kwargs)

    if fe_type == "Relboost":
        return Relboost(**kwargs)

    if fe_type == "RelMT":
        return RelMT(**kwargs)

    raise ValueError("Unknown feature learning algorithm: " + fe_type)


# --------------------------------------------------------------------


def _parse_metadata(dict_: Dict[str, Any]) -> Metadata:
    return Metadata(name=dict_["name"], roles=Roles(**dict_["roles"]))


# --------------------------------------------------------------------


def _parse_pred(
    dict_: Dict[str, Any],
) -> Union[
    ScaleGBMClassifier,
    ScaleGBMRegressor,
    LinearRegression,
    LogisticRegression,
    XGBoostClassifier,
    XGBoostRegressor,
]:
    kwargs = _remove_trailing_underscores(dict_)

    pred_type = kwargs["type"]

    del kwargs["type"]

    if pred_type == "LinearRegression":
        return LinearRegression(**kwargs)

    if pred_type == "LogisticRegression":
        return LogisticRegression(**kwargs)

    if pred_type == "ScaleGBMClassifier":
        return ScaleGBMClassifier(**kwargs)

    if pred_type == "ScaleGBMRegressor":
        return ScaleGBMRegressor(**kwargs)

    if pred_type == "XGBoostClassifier":
        return XGBoostClassifier(**kwargs)

    if pred_type == "XGBoostRegressor":
        return XGBoostRegressor(**kwargs)

    raise ValueError("Unknown predictor: " + pred_type)


# --------------------------------------------------------------------


def _parse_preprocessor(
    dict_: Dict[str, Any],
) -> Union[
    CategoryTrimmer,
    EmailDomain,
    Imputation,
    Mapping,
    Seasonal,
    Substring,
    TextFieldSplitter,
]:
    kwargs = _remove_trailing_underscores(dict_)

    pre_type = kwargs["type"]

    del kwargs["type"]

    if pre_type == "CategoryTrimmer":
        return CategoryTrimmer(**kwargs)

    if pre_type == "EmailDomain":
        return EmailDomain()

    if pre_type == "Imputation":
        return Imputation(**kwargs)

    if pre_type == "Mapping":
        return Mapping(**kwargs)

    if pre_type == "Seasonal":
        return Seasonal()

    if pre_type == "Substring":
        return Substring(**kwargs)

    if pre_type == "TextFieldSplitter":
        return TextFieldSplitter()

    raise ValueError("Unknown preprocessor: " + pre_type)


# --------------------------------------------------------------------


def _print_time_taken(begin: float, end: float, msg: str) -> None:
    """Prints time required to fit a model.

    Args:
        begin (float): [`time`][time.time] output marking the beginning
            of the training.
        end (float): [`time`][time.time] output marking the end of the
            training.
        msg (str): Message to display along the duration.

    """

    if not isinstance(begin, float):
        raise TypeError("'begin' must be a float as returned by time.time().")

    if not isinstance(end, float):
        raise TypeError("'end' must be a float as returned by time.time().")

    if not isinstance(msg, str):
        raise TypeError("'msg' must be a str.")

    seconds = end - begin

    delta = timedelta(seconds=seconds)

    builtins.print(f"Time taken: {delta}.\n")


# --------------------------------------------------------------------


def _replace_with_nan_maybe(dict_: Dict[str, Any]) -> Dict[str, Any]:
    dict_ = deepcopy(dict_)

    for key, values in dict_.items():
        if isinstance(values, list):
            dict_[key] = [np.nan if value == -1 else value for value in values]

    return dict_


# --------------------------------------------------------------------


def _transform_peripheral(
    peripheral_tables: Optional[
        Union[
            Sequence[Union[DataFrame, View]],
            Dict[str, Union[DataFrame, View]],
            Union[DataFrame, View],
        ]
    ],
    placeholders: List[Placeholder],
) -> Sequence[Union[DataFrame, View]]:
    """
    Transforms the peripheral_tables into the desired list
    format
    """
    peripheral_tables = peripheral_tables or []

    if isinstance(peripheral_tables, dict):
        peripheral_tables = _extract_dfs(peripheral_tables, placeholders)

    if isinstance(peripheral_tables, (DataFrame, View)):
        peripheral_tables = [peripheral_tables]

    return peripheral_tables


# --------------------------------------------------------------------


def _unlist_maybe(list_: list) -> Union[List[Any], Any]:
    """
    `_unlist_maybe` has an unstable return type and should therefore only
    be used in interactive contexts. An example are a pipeline's atomic scores
    properties (auc, accuracy, ...) that are just convenience wrappers for
    interactive sessions.
    """
    if len(list_) == 1:
        return list_[0]
    return list_


# ------------------------------------------------------------
