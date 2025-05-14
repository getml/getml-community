# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains routines for preprocessing data frames.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .preprocessor import _Preprocessor
from .validate import _validate


@dataclass(repr=False)
class Imputation(_Preprocessor):
    """
    The Imputation preprocessor replaces all NULL values in
    numerical columns with the mean of the remaining
    columns.

    Optionally, it can additionally add a dummy column
    that signifies whether the original value was imputed.

    Args:
        add_dummies:
            Whether you want to add dummy variables
            that signify whether the original value was imputed.

    ??? example
        ```python
        imputation = getml.preprocessors.Imputation()

        pipe = getml.Pipeline(
            population=population_placeholder,
            peripheral=[order_placeholder, trans_placeholder],
            preprocessors=[imputation],
            feature_learners=[feature_learner_1, feature_learner_2],
            feature_selectors=feature_selector,
            predictors=predictor,
            share_selected_features=0.5
        )
        ```
    """

    add_dummies: bool = False

    def validate(self, params: Optional[Dict[str, Any]] = None) -> None:
        """Checks both the types and the values of all instance
        variables and raises an exception if something is off.

        Args:
            params:
                A dictionary containing
                the parameters to validate. If not is passed,
                the own parameters will be validated.
        """
        params = _validate(self, params)

        if not isinstance(params["add_dummies"], bool):
            raise TypeError("'add_dummies' must be a bool.")
