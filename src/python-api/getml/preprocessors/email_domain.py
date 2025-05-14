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
class EmailDomain(_Preprocessor):
    """
    The EmailDomain preprocessor extracts the domain from e-mail addresses.

    For instance, if the e-mail address is 'some.guy@domain.com',
    the preprocessor will automatically extract '@domain.com'.

    The preprocessor will be applied to all [`text`][getml.data.roles.text]
    columns that were assigned one of the [`subroles`][getml.data.subroles]
    [`include.email`][getml.data.subroles.include.email] or
    [`only.email`][getml.data.subroles.only.email].

    It is recommended that you assign [`only.email`][getml.data.subroles.only.email],
    because it is unlikely that the e-mail address itself is interesting.

    ??? example
        ```python
        my_data_frame.set_subroles("email", getml.data.subroles.only.email)

        domain = getml.preprocessors.EmailDomain()

        pipe = getml.Pipeline(
            population=population_placeholder,
            peripheral=[order_placeholder, trans_placeholder],
            preprocessors=[domain],
            feature_learners=[feature_learner_1, feature_learner_2],
            feature_selectors=feature_selector,
            predictors=predictor,
            share_selected_features=0.5
        )
        ```
    """

    def validate(self, params: Optional[Dict[str, Any]] = None) -> None:
        """Checks both the types and the values of all instance
        variables and raises an exception if something is off.

        Args:
            params:
                A dictionary containing
                the parameters to validate. If not is passed,
                the own parameters will be validated.
        """
        _validate(self, params)
