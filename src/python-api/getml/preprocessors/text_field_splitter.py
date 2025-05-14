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


# NOTE: The r at the beginning of the docstring
# is necessary to correctly display the characters.
# https://docutils.sourceforge.io/docs/ref/rst/directives.html#code
@dataclass(repr=False)
class TextFieldSplitter(_Preprocessor):
    r"""
    A TextFieldSplitter splits columns with role [`text`][getml.data.roles.text]
    into relational bag-of-words representations to allow the
    feature learners to learn patterns based on
    the prescence of certain words within the text fields.

    Text fields will be split on a whitespace or any of the
    following characters:

    ```python
    ; , . ! ? - | " \t \v \f \r \n % ' ( ) [ ] { }
    ```
    Refer to the [User Guide][preprocessing-free-form-text] for more information.

    ??? example
        ```python
        text_field_splitter = getml.preprocessors.TextFieldSplitter()

        pipe = getml.Pipeline(
            population=population_placeholder,
            peripheral=[order_placeholder, trans_placeholder],
            preprocessors=[text_field_splitter],
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
