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
class Substring(_Preprocessor):
    """
    The Substring preprocessor extracts substrings from
    categorical columns and unused string columns.

    The preprocessor will be applied to all
    :const:`~getml.data.roles.categorical` and :const:`~getml.data.roles.text`
    columns that were assigned one of the :mod:`~getml.data.subroles`
    :const:`getml.data.subroles.include.substring` or
    :const:`getml.data.subroles.only.substring`.

    To further limit the scope of a substring preprocessor,
    you can also assign a *unit*.

    Args:
        begin (int):
            Index of the beginning of the substring (starting from 0).

        length (int):
            The length of the substring.

        unit (str, optional):
            The unit of all columns to which the proprocessor
            should be applied. These columns must also have the subrole
            substring.

            If it is left empty, then the preprocessor
            will be applied to all columns with the subrole
            :const:`getml.data.subroles.include.substring` or
            :const:`getml.data.subroles.only.substring`.

    Example:
        .. code-block:: python

            my_df.set_subroles("col1", getml.data.subroles.include.substring)

            my_df.set_subroles("col2", getml.data.subroles.include.substring)
            my_df.set_unit("col2", "substr14")

            # Will be applied to col1 and col2
            substr13 = getml.preprocessors.Substring(0, 3)

            # Will only be applied to col2
            substr14 = getml.preprocessors.Substring(0, 3, "substr14")

            pipe = getml.Pipeline(
                population=population_placeholder,
                peripheral=[order_placeholder, trans_placeholder],
                preprocessors=[substr13],
                feature_learners=[feature_learner_1, feature_learner_2],
                feature_selectors=feature_selector,
                predictors=predictor,
                share_selected_features=0.5
            )
    """

    begin: int
    length: int
    unit: str = ""

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

        if not isinstance(params["begin"], int):
            raise TypeError("'begin' must be an integer!")

        if not isinstance(params["length"], int):
            raise TypeError("'length' must be an integer!")

        if not isinstance(params["unit"], str):
            raise TypeError("'unit' must be a string!")

        if params["begin"] < 0:
            raise ValueError("'begin' must be >= 0!")

        if params["length"] <= 0:
            raise ValueError("'length' must be > 0!")
