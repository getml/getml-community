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
class Seasonal(_Preprocessor):
    """
    The Seasonal preprocessor extracts seasonal data from time stamps.

    The preprocessor automatically iterates through
    all time stamps in any data frame and extracts
    seasonal parameters.

    These include:

        - year
        - month
        - weekday
        - hour
        - minute

    The algorithm also evaluates the potential
    usefulness of any extracted seasonal parameter.
    Parameters that are unlikely to be useful are
    not included.

    Example:
        .. code-block:: python

            seasonal = getml.preprocessors.Seasonal()

            pipe = getml.Pipeline(
                population=population_placeholder,
                peripheral=[order_placeholder, trans_placeholder],
                preprocessors=[seasonal],
                feature_learners=[feature_learner_1, feature_learner_2],
                feature_selectors=feature_selector,
                predictors=predictor,
                share_selected_features=0.5
            )
    """

    def validate(self, params=None):
        """Checks both the types and the values of all instance
        variables and raises an exception if something is off.

        Args:
            params (dict, optional):
                A dictionary containing
                the parameters to validate. If not is passed,
                the own parameters will be validated.
        """
        _validate(self, params)
