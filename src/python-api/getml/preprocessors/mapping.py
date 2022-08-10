# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains routines for preprocessing data frames.
"""

from dataclasses import dataclass, field
from typing import ClassVar, List

from getml.feature_learning.aggregations import mapping as mapping_aggregations
from getml.feature_learning.aggregations import _Aggregations

from .validate import _validate
from .preprocessor import _Preprocessor


@dataclass(repr=False)
class Mapping(_Preprocessor):
    """
    A mapping preprocessor maps categorical values, discrete values and individual
    words in a text field to numerical values. These numerical values are retrieved
    by aggregating targets in the relational neighbourhood.

    You are particularly encouraged to use the mapping preprocessor in combination with
    :class:`~getml.feature_learning.FastProp`.

    Refer to the :ref:`User guide <mappings>` for more information.

    Args:
        aggregation (List[:class:`~getml.feature_learning.aggregations`], optional):
            The aggregation function to use over the targets.

            Must be from :mod:`~getml.feature_learning.aggregations`.

        min_freq (int, optional):
            The minimum number of targets required for a value to be included in
            the mapping. Range: [0, :math:`\\infty`]

        multithreading (bool, optional):
            Whether you want to apply multithreading.

    Example:
        .. code-block:: python

            mapping = getml.preprocessors.Mapping()

            pipe = getml.Pipeline(
                population=population_placeholder,
                peripheral=[order_placeholder, trans_placeholder],
                preprocessors=[mapping],
                feature_learners=[feature_learner_1, feature_learner_2],
                feature_selectors=feature_selector,
                predictors=predictor,
                share_selected_features=0.5
            )

    Note:
        Not supported in the getML community edition.
    """

    agg_sets: ClassVar[_Aggregations] = mapping_aggregations

    aggregation: List[str] = field(default_factory=lambda: mapping_aggregations.Default)
    min_freq: int = 30
    multithreading: bool = True

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

        if not all([agg in mapping_aggregations.All for agg in params["aggregation"]]):
            raise ValueError(
                "'aggregation' must be from Mapping.agg_sets.All, "
                + "meaning from the following set: "
                + str(mapping_aggregations.All)
                + "."
            )

        if not isinstance(params["min_freq"], int):
            raise TypeError("'min_freq' must be an int.")

        if params["min_freq"] < 0:
            raise TypeError("'min_freq' cannot be negative.")

        if not isinstance(params["multithreading"], bool):
            raise TypeError("'multithreading' must be a bool.")
