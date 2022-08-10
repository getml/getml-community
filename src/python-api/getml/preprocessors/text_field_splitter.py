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


# NOTE: The r at the beginning of the docstring
# is necessary to correctly display the characters.
# https://docutils.sourceforge.io/docs/ref/rst/directives.html#code
@dataclass(repr=False)
class TextFieldSplitter(_Preprocessor):
    r"""
    A TextFieldSplitter splits columns with role :const:`getml.data.roles.text`
    into relational bag-of-words representations to allow the
    feature learners to learn patterns based on
    the prescence of certain words within the text fields.

    Text fields will be splitted on a whitespace or any of the
    following characters:

    .. code:: python

        ; , . ! ? - | " \t \v \f \r \n % ' ( ) [ ] { }

    Refer to the :ref:`User guide <text_fields>` for more information.

    Example:
        .. code-block:: python

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
