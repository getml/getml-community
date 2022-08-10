# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains routines for preprocessing data frames.
"""

from dataclasses import dataclass

from .preprocessor import _Preprocessor
from .validate import _validate


@dataclass(repr=False)
class CategoryTrimmer(_Preprocessor):
    """
    Reduces the cardinality of high-cardinality categorical columns.

    Args:
        max_num_categories (int, optional):
            The maximum cardinality allowed. If the cardinality is
            higher than that only the most frequent categories will
            be kept, all others will be trimmed.

        min_freq (int, optional):
            The minimum frequency required for a category to be
            included.

    Example:
        .. code-block:: python

            category_trimmer = getml.preprocessors.CategoryTrimmer()

            pipe = getml.Pipeline(
                population=population_placeholder,
                peripheral=[order_placeholder, trans_placeholder],
                preprocessors=[category_trimmer],
                feature_learners=[feature_learner_1, feature_learner_2],
                feature_selectors=feature_selector,
                predictors=predictor,
                share_selected_features=0.5
            )
    """

    max_num_categories: int = 999
    min_freq: int = 30

    def validate(self, params=None):
        """Checks both the types and the values of all instance
        variables and raises an exception if something is off.

        Args:
            params (dict, optional):
                A dictionary containing
                the parameters to validate. If not is passed,
                the own parameters will be validated.
        """
        params = _validate(self, params)

        if not isinstance(params["max_num_categories"], int):
            raise TypeError("'max_num_categories' must be an int.")

        if not isinstance(params["min_freq"], int):
            raise TypeError("'min_freq' must be an int.")

        if params["max_num_categories"] < 0:
            raise ValueError("'max_num_categories' cannot be negative.")

        if params["min_freq"] < 0:
            raise ValueError("'min_freq' cannot be negative.")
