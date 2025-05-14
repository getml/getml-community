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
from typing import Any, ClassVar, Dict, Iterable, Optional

from ..feature_learning.aggregations.sets import (
    MAPPING,
    MappingAggregationsSets,
)
from ..feature_learning.aggregations.types import MappingAggregations
from .preprocessor import _Preprocessor
from .validate import _validate


@dataclass(repr=False)
class Mapping(_Preprocessor):
    """
    A mapping preprocessor maps categorical values, discrete values and individual
    words in a text field to numerical values. These numerical values are retrieved
    by aggregating targets in the relational neighbourhood.

    You are particularly encouraged to use the mapping preprocessor in combination with
    [`FastProp`][getml.feature_learning.FastProp].

    Refer to the [User guide][preprocessing-mappings] for more information.

    enterprise-adm: Enterprise edition
        This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

        For licensing information and technical support, please [contact us][contact-page].

    Attributes:
        agg_sets:
            It is a class variable holding the available aggregation sets for the
            mapping preprocessor.
            Value: [`MAPPING`][getml.feature_learning.aggregations.sets.MAPPING].

    Parameters:
        aggregation:
            The aggregation function to use over the targets.

            Must be an aggregation supported by Mapping preprocessor
            ([`MAPPING_AGGREGATIONS`][getml.feature_learning.aggregations.sets.MAPPING_AGGREGATIONS]).

        min_freq:
            The minimum number of targets required for a value to be included in
            the mapping. Range: [0, ∞]

        multithreading:
            Whether you want to apply multithreading.

    ??? example
        ```python
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
        ```

    """

    agg_sets: ClassVar[MappingAggregationsSets] = MAPPING

    aggregation: Iterable[MappingAggregations] = MAPPING.default
    min_freq: int = 30
    multithreading: bool = True

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

        if not all([agg in MAPPING.all for agg in params["aggregation"]]):
            raise ValueError(
                "'aggregation' must be from Mapping.agg_sets.all, "
                + "meaning from the following set: "
                + str(MAPPING.all)
                + "."
            )

        if not isinstance(params["min_freq"], int):
            raise TypeError("'min_freq' must be an int.")

        if params["min_freq"] < 0:
            raise TypeError("'min_freq' cannot be negative.")

        if not isinstance(params["multithreading"], bool):
            raise TypeError("'multithreading' must be a bool.")
