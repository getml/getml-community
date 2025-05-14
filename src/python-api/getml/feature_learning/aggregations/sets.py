# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

from dataclasses import dataclass
from typing import FrozenSet

from getml.feature_learning.aggregations.aggregations import (
    AVG,
    COUNT,
    COUNT_DISTINCT,
    COUNT_DISTINCT_OVER_COUNT,
    COUNT_MINUS_COUNT_DISTINCT,
    EWMA_1D,
    EWMA_1H,
    EWMA_1M,
    EWMA_1S,
    EWMA_7D,
    EWMA_30D,
    EWMA_90D,
    EWMA_365D,
    EWMA_TREND_1D,
    EWMA_TREND_1H,
    EWMA_TREND_1M,
    EWMA_TREND_1S,
    EWMA_TREND_7D,
    EWMA_TREND_30D,
    EWMA_TREND_90D,
    EWMA_TREND_365D,
    FIRST,
    KURTOSIS,
    LAST,
    MAX,
    MEDIAN,
    MIN,
    MODE,
    NUM_MAX,
    NUM_MIN,
    Q_1,
    Q_5,
    Q_10,
    Q_25,
    Q_75,
    Q_90,
    Q_95,
    Q_99,
    SKEW,
    STDDEV,
    SUM,
    TIME_SINCE_FIRST_MAXIMUM,
    TIME_SINCE_FIRST_MINIMUM,
    TIME_SINCE_LAST_MAXIMUM,
    TIME_SINCE_LAST_MINIMUM,
    TREND,
    VAR,
    VARIATION_COEFFICIENT,
)
from getml.feature_learning.aggregations.types import (
    Aggregations,
    FastPropAggregations,
    MappingAggregations,
    MultirelAggregations,
)

MULTIREL_AGGREGATIONS: FrozenSet[MultirelAggregations] = frozenset(
    {
        AVG,
        COUNT,
        COUNT_DISTINCT,
        COUNT_MINUS_COUNT_DISTINCT,
        FIRST,
        LAST,
        MAX,
        MEDIAN,
        MIN,
        STDDEV,
        SUM,
        VAR,
    }
)
"""Set of all aggregations supported by Multirel."""

FASTPROP_AGGREGATIONS: FrozenSet[FastPropAggregations] = frozenset(
    {
        AVG,
        COUNT,
        COUNT_DISTINCT,
        COUNT_MINUS_COUNT_DISTINCT,
        FIRST,
        LAST,
        MAX,
        MEDIAN,
        MIN,
        STDDEV,
        SUM,
        VAR,
        COUNT_DISTINCT_OVER_COUNT,
        EWMA_1S,
        EWMA_1M,
        EWMA_1H,
        EWMA_1D,
        EWMA_7D,
        EWMA_30D,
        EWMA_90D,
        EWMA_365D,
        EWMA_TREND_1S,
        EWMA_TREND_1M,
        EWMA_TREND_1H,
        EWMA_TREND_1D,
        EWMA_TREND_7D,
        EWMA_TREND_30D,
        EWMA_TREND_90D,
        EWMA_TREND_365D,
        KURTOSIS,
        MODE,
        NUM_MAX,
        NUM_MIN,
        Q_1,
        Q_5,
        Q_10,
        Q_25,
        Q_75,
        Q_90,
        Q_95,
        Q_99,
        SKEW,
        TIME_SINCE_FIRST_MAXIMUM,
        TIME_SINCE_FIRST_MINIMUM,
        TIME_SINCE_LAST_MAXIMUM,
        TIME_SINCE_LAST_MINIMUM,
        TREND,
        VARIATION_COEFFICIENT,
    }
)
"""Set of all aggregations supported by FastProp."""

# ------------------------------------------------------------------------------

MAPPING_AGGREGATIONS: FrozenSet[MappingAggregations] = frozenset(
    {
        AVG,
        COUNT,
        COUNT_DISTINCT,
        COUNT_DISTINCT_OVER_COUNT,
        COUNT_MINUS_COUNT_DISTINCT,
        KURTOSIS,
        MAX,
        MEDIAN,
        MIN,
        MODE,
        NUM_MAX,
        NUM_MIN,
        Q_1,
        Q_5,
        Q_10,
        Q_25,
        Q_75,
        Q_90,
        Q_95,
        Q_99,
        SKEW,
        STDDEV,
        SUM,
        VAR,
        VARIATION_COEFFICIENT,
    }
)
"""Set of all aggregations supported by the mapping preprocessor."""

# ------------------------------------------------------------------------------

AGGREGATIONS: FrozenSet[Aggregations] = (
    MULTIREL_AGGREGATIONS | FASTPROP_AGGREGATIONS | MAPPING_AGGREGATIONS
)
"""Set of all possible aggregations."""

# ------------------------------------------------------------------------------


@dataclass(frozen=True)
class AggregationsSets:
    """
    Base Class for aggregations sets
    """

    all: FrozenSet[Aggregations]
    default: FrozenSet[Aggregations]
    minimal: FrozenSet[Aggregations]

    # Introduce CamelCase aliases to keep backward compatibility
    @property
    def All(self) -> FrozenSet[Aggregations]:
        return self.all

    @property
    def Default(self) -> FrozenSet[Aggregations]:
        return self.default

    @property
    def Minimal(self) -> FrozenSet[Aggregations]:
        return self.minimal


@dataclass(frozen=True)
class MappingAggregationsSets(AggregationsSets):
    """
    Base class for Mapping aggregation sets
    """

    all: FrozenSet[MappingAggregations]
    default: FrozenSet[MappingAggregations]
    minimal: FrozenSet[MappingAggregations]


@dataclass(frozen=True)
class FastPropAggregationsSets(AggregationsSets):
    """
    Base class for FastProp aggregation sets
    """

    all: FrozenSet[FastPropAggregations]
    default: FrozenSet[FastPropAggregations]
    minimal: FrozenSet[FastPropAggregations]


@dataclass(frozen=True)
class MultirelAggregationsSets(AggregationsSets):
    """
    Base class for Multirel aggregation sets
    """

    all: FrozenSet[MultirelAggregations]
    default: FrozenSet[MultirelAggregations]
    minimal: FrozenSet[MultirelAggregations]


FASTPROP = FastPropAggregationsSets(
    all=FASTPROP_AGGREGATIONS,
    default=frozenset(
        {
            AVG,
            COUNT,
            COUNT_DISTINCT,
            COUNT_MINUS_COUNT_DISTINCT,
            FIRST,
            LAST,
            MAX,
            MEDIAN,
            MIN,
            MODE,
            STDDEV,
            SUM,
            TREND,
        }
    ),
    minimal=frozenset([AVG, COUNT, MAX, MIN, SUM]),
)
"""Set of default aggregations for [`FastProp`][getml.feature_learning.FastProp]. 
`all` contains all aggregations supported by FastProp, `default` contains the subset 
of reasonable default aggregations, `minimal` is minimal set."""

# ------------------------------------------------------------------------------

MULTIREL = MultirelAggregationsSets(
    all=MULTIREL_AGGREGATIONS,
    default=frozenset(
        {
            AVG,
            COUNT,
            MAX,
            MIN,
            SUM,
        }
    ),
    minimal=frozenset([AVG, COUNT, SUM]),
)
"""Set of default aggregations for [`Multirel`][getml.feature_learning.Multirel]. 
`all` contains all aggregations supported by Multirel, `default` contains the subset 
of reasonable default aggregations, `minimal` is minimal set."""

# ------------------------------------------------------------------------------

MAPPING = MappingAggregationsSets(
    all=MAPPING_AGGREGATIONS,
    default=frozenset({AVG}),
    minimal=frozenset({AVG}),
)
"""Set of default aggregations for [`Mapping`][getml.preprocessors.Mapping]. `all` 
contains all aggregations supported by the mapping preprocessor. `default` and 
`minimal` are identical and include only the AVG aggregation, which is the 
recommended setting for classification problems."""

# for backward compatibility of internal tests
_all_aggregations = AGGREGATIONS
_multirel_subset = MULTIREL_AGGREGATIONS
