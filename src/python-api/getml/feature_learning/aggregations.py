# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
This module contains all possible aggregations to be used with
[`Multirel`][getml.feature_learning.Multirel],
[`FastProp`][getml.feature_learning.FastProp],
[`Mapping`][getml.preprocessors.Mapping].


Refer to the [feature learning section][feature_learning] in the user guide for details about how
these aggregations are used in the context of feature learning.
"""

from collections import namedtuple
from typing import Literal, Final, NamedTuple, FrozenSet

# ------------------------------------------------------------------------------

Avg = Literal["AVG"]
AVG: Final[Avg] = "AVG"
"""
Average value of a given numerical column.
"""
Count = Literal["COUNT"]
COUNT: Final[Count] = "COUNT"
"""
Number of rows in a given column.
"""

CountDistinct = Literal["COUNT DISTINCT"]
COUNT_DISTINCT: Final[CountDistinct] = "COUNT DISTINCT"


CountDistinctOverCount = Literal["COUNT DISTINCT OVER COUNT"]
COUNT_DISTINCT_OVER_COUNT: Final[CountDistinctOverCount] = "COUNT DISTINCT OVER COUNT"
"""
COUNT DISTINCT divided by COUNT.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

CountMinusCountDistinct = Literal["COUNT MINUS COUNT DISTINCT"]
COUNT_MINUS_COUNT_DISTINCT: Final[
    CountMinusCountDistinct
] = "COUNT MINUS COUNT DISTINCT"
"""
Counts minus counts distinct. Substracts COUNT DISTINCT from COUNT.
"""

EWMA_1s = Literal["EWMA_1S"]
EWMA_1S: Final[EWMA_1s] = "EWMA_1S"
"""
Exponentially weighted moving average with a half-life of 1 second.
"""

EWMA_1m = Literal["EWMA_1M"]
EWMA_1M: Final[EWMA_1m] = "EWMA_1M"
"""
Exponentially weighted moving average with a half-life of 1 minute.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

EWMA_1h = Literal["EWMA_1H"]
EWMA_1H: Final[EWMA_1h] = "EWMA_1H"
"""
Exponentially weighted moving average with a half-life of 1 hour.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

EWMA_1d = Literal["EWMA_1D"]
EWMA_1D: Final[EWMA_1d] = "EWMA_1D"
"""
Exponentially weighted moving average with a half-life of 1 day.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

EWMA_7d = Literal["EWMA_7D"]
EWMA_7D: Final[EWMA_7d] = "EWMA_7D"
"""
Exponentially weighted moving average with a half-life of 7 days.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

EWMA_30d = Literal["EWMA_30D"]
EWMA_30D: Final[EWMA_30d] = "EWMA_30D"
"""
Exponentially weighted moving average with a half-life of 30 days.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

EWMA_90d = Literal["EWMA_90D"]
EWMA_90D: Final[EWMA_90d] = "EWMA_90D"
"""
Exponentially weighted moving average with a half-life of 90 days.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

EWMA_365d = Literal["EWMA_365D"]
EWMA_365D: Final[EWMA_365d] = "EWMA_365D"
"""
Exponentially weighted moving average with a half-life of 365 days.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

EWMA_TREND_1s = Literal["EWMA_TREND_1S"]
EWMA_TREND_1S: Final[EWMA_TREND_1s] = "EWMA_TREND_1S"
"""
Exponentially weighted trend with a half-life of 1 second.
"""

EWMA_TREND_1m = Literal["EWMA_TREND_1M"]
EWMA_TREND_1M: Final[EWMA_TREND_1m] = "EWMA_TREND_1M"
"""
Exponentially weighted trend with a half-life of 1 minute.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

EWMA_TREND_1h = Literal["EWMA_TREND_1H"]
EWMA_TREND_1H: Final[EWMA_TREND_1h] = "EWMA_TREND_1H"
"""
Exponentially weighted trend with a half-life of 1 hour.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

EWMA_TREND_1d = Literal["EWMA_TREND_1D"]
EWMA_TREND_1D: Final[EWMA_TREND_1d] = "EWMA_TREND_1D"
"""
Exponentially weighted trend with a half-life of 1 day.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

EWMA_TREND_7d = Literal["EWMA_TREND_7D"]
EWMA_TREND_7D: Final[EWMA_TREND_7d] = "EWMA_TREND_7D"
"""
Exponentially weighted trend with a half-life of 7 days.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

EWMA_TREND_30d = Literal["EWMA_TREND_30D"]
EWMA_TREND_30D: Final[EWMA_TREND_30d] = "EWMA_TREND_30D"
"""
Exponentially weighted trend with a half-life of 30 days.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

EWMA_TREND_90d = Literal["EWMA_TREND_90D"]
EWMA_TREND_90D: Final[EWMA_TREND_90d] = "EWMA_TREND_90D"
"""
Exponentially weighted trend with a half-life of 90 days.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

EWMA_TREND_365d = Literal["EWMA_TREND_365D"]
EWMA_TREND_365D: Final[EWMA_TREND_365d] = "EWMA_TREND_365D"
"""
Exponentially weighted trend with a half-life of 365 days.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

First = Literal["FIRST"]
FIRST: Final[First] = "FIRST"
"""
First value of a given column, when ordered by the time stamp.
"""
Kurtosis = Literal["KURTOSIS"]
KURTOSIS: Final[Kurtosis] = "KURTOSIS"

"""
The kurtosis of a given column.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

Last = Literal["LAST"]
LAST: Final[Last] = "LAST"
"""
Last value of a given column, when ordered by the time stamp.
"""

Max = Literal["MAX"]
MAX: Final[Max] = "MAX"
"""
Largest value of a given column.
"""

Median = Literal["MEDIAN"]
MEDIAN: Final[Median] = "MEDIAN"
"""
Median of a given column.
"""

Min = Literal["MIN"]
MIN: Final[Min] = "MIN"
"""
Smallest value of a given column.
"""

Mode = Literal["MODE"]
MODE: Final[Mode] = "MODE"
"""
Most frequent value of a given column.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

NumMax = Literal["NUM MAX"]
NUM_MAX: Final[NumMax] = "NUM MAX"
"""
The number of times we observe the maximum value.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

NumMin = Literal["NUM MIN"]
NUM_MIN: Final[NumMin] = "NUM MIN"
"""
The number of times we observe the minimum value.
Please note that this aggregation is
not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

Q1 = Literal["Q1"]
Q_1: Final[Q1] = "Q1"
"""
The 1%-quantile.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

Q5 = Literal["Q5"]
Q_5: Final[Q5] = "Q5"
"""
The 5%-quantile.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

Q10 = Literal["Q10"]
Q_10: Final[Q10] = "Q10"
"""
The 10%-quantile.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

Q25 = Literal["Q25"]
Q_25: Final[Q25] = "Q25"
"""
The 25%-quantile.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

Q75 = Literal["Q75"]
Q_75: Final[Q75] = "Q75"
"""
The 75%-quantile.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

Q90 = Literal["Q90"]
Q_90: Final[Q90] = "Q90"
"""
The 90%-quantile.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

Q95 = Literal["Q95"]
Q_95: Final[Q95] = "Q95"
"""
The 95%-quantile.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

Q99 = Literal["Q99"]
Q_99: Final[Q99] = "Q99"
"""
The 99%-quantile.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

Skew = Literal["SKEW"]
SKEW: Final[Skew] = "SKEW"
"""
Skewness of a given column.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

Stddev = Literal["STDDEV"]
STDDEV: Final[Stddev] = "STDDEV"
"""
Standard deviation of a given column.
"""

Sum = Literal["SUM"]
SUM: Final[Sum] = "SUM"
"""
Total sum of a given numerical column.
"""

TimeSinceFirstMaximum = Literal["TIME SINCE FIRST MAXIMUM"]
TIME_SINCE_FIRST_MAXIMUM: Final[TimeSinceFirstMaximum] = "TIME SINCE FIRST MAXIMUM"
"""
The time difference between the first time we see
the maximum value and the time stamp in the population table.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

TimeSinceFirstMinimum = Literal["TIME SINCE FIRST MINIMUM"]
TIME_SINCE_FIRST_MINIMUM: Final[TimeSinceFirstMinimum] = "TIME SINCE FIRST MINIMUM"
"""
The time difference between the first time we see
the minimum value and the time stamp in the population table.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

TimeSinceLastMaximum = Literal["TIME SINCE LAST MAXIMUM"]
TIME_SINCE_LAST_MAXIMUM: Final[TimeSinceLastMaximum] = "TIME SINCE LAST MAXIMUM"
"""
The time difference between the last time we see
the maximum value and the time stamp in the population table.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

TimeSinceLastMinimum = Literal["TIME SINCE LAST MINIMUM"]
TIME_SINCE_LAST_MINIMUM: Final[TimeSinceLastMinimum] = "TIME SINCE LAST MINIMUM"
"""
The time difference between the last time we see
the minimum value and the time stamp in the population table.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

Trend = Literal["TREND"]
TREND: Final[Trend] = "TREND"
"""
Extracts a linear trend from a variable over time and
extrapolates this trend to the current time stamp.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

Var = Literal["VAR"]
VAR: Final[Var] = "VAR"
"""
Statistical variance of a given numerical column.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""

VariationCoefficient = Literal["VARIATION COEFFICIENT"]
VARIATION_COEFFICIENT: Final[VariationCoefficient] = "VARIATION COEFFICIENT"
"""
VAR divided by MEAN.
Please note that this aggregation is not supported by
[`Multirel`][getml.feature_learning.Multirel].
"""


# ------------------------------------------------------------------------------

_multirel_subset = [
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
]


# ------------------------------------------------------------------------------

_additional_aggregations_for_fast_prop = [
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
]

# ------------------------------------------------------------------------------

_mapping_subset = [
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
]

# ------------------------------------------------------------------------------

_all_aggregations = _multirel_subset + _additional_aggregations_for_fast_prop

# ------------------------------------------------------------------------------

__multirel_subset = [
    Avg,
    Count,
    CountDistinct,
    CountMinusCountDistinct,
    First,
    Last,
    Max,
    Median,
    Min,
    Stddev,
    Sum,
    Var,
]


__additional_aggregations_for_fast_prop = [
    CountDistinctOverCount,
    EWMA_1s,
    EWMA_1m,
    EWMA_1h,
    EWMA_1d,
    EWMA_7d,
    EWMA_30d,
    EWMA_90d,
    EWMA_365d,
    EWMA_TREND_1s,
    EWMA_TREND_1m,
    EWMA_TREND_1h,
    EWMA_TREND_1d,
    EWMA_TREND_7d,
    EWMA_TREND_30d,
    EWMA_TREND_90d,
    EWMA_TREND_365d,
    Kurtosis,
    Mode,
    NumMax,
    NumMin,
    Q1,
    Q5,
    Q10,
    Q25,
    Q75,
    Q90,
    Q95,
    Q99,
    Skew,
    TimeSinceFirstMaximum,
    TimeSinceFirstMinimum,
    TimeSinceLastMaximum,
    TimeSinceLastMinimum,
    Trend,
    VariationCoefficient,
]

__mapping_subset = [
    Avg,
    Count,
    CountDistinct,
    CountDistinctOverCount,
    CountMinusCountDistinct,
    Kurtosis,
    Max,
    Median,
    Min,
    Mode,
    NumMax,
    NumMin,
    Q1,
    Q5,
    Q10,
    Q25,
    Q75,
    Q90,
    Q95,
    Q99,
    Skew,
    Stddev,
    Sum,
    Var,
    VariationCoefficient,
]
__all_aggregations = __multirel_subset + __additional_aggregations_for_fast_prop


class Aggregations(NamedTuple):
    all: FrozenSet = frozenset()
    default: FrozenSet = frozenset()
    minimal: FrozenSet = frozenset()


fastprop_aggregations = Aggregations(
    all=frozenset(__all_aggregations),
    default=frozenset(
        [
            Avg,
            Count,
            CountDistinct,
            CountMinusCountDistinct,
            First,
            Last,
            Max,
            Median,
            Min,
            Mode,
            Stddev,
            Sum,
            Trend,
        ]
    ),
    minimal=frozenset([Avg, Count, Max, Min, Sum]),
)

multirel_aggregations = Aggregations(
    all=frozenset(__multirel_subset),
    default=frozenset([Avg, Count, Max, Min, Sum]),
    minimal=frozenset([Avg, Count, Sum]),
)

mapping_aggregations = Aggregations(
    all=frozenset(__mapping_subset), default=frozenset([Avg]), minimal=frozenset([Avg])
)

_Aggregations = namedtuple("_Aggregations", ["All", "Default", "Minimal"])

# ------------------------------------------------------------------------------

fastprop = _Aggregations(
    All=_all_aggregations,
    Default=[
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
    ],
    Minimal=[AVG, COUNT, MAX, MIN, SUM],
)
"""
Set of default aggregations for [`FastProp`][getml.feature_learning.FastProp] and
[`FastPropTimeseries`][getml.feature_learning.FastPropTimeseries]. `All` contains all aggregations
supported by FastProp, `Default` contains the subset of reasonable default aggregations,
`Minimal` is minimal set.
"""

# ------------------------------------------------------------------------------

mapping = _Aggregations(
    All=_mapping_subset,
    Default=[AVG],
    Minimal=[AVG],
)
"""
Set of default aggregations for [`Mapping`][getml.preprocessors.mapping]. `All` contains all
aggregations supported by the mapping preprocessor. `Default` and `Minimal` are identical
and include only the AVG aggregation, which is the recommended setting for classification
problems.
"""

# ------------------------------------------------------------------------------

multirel = _Aggregations(
    All=_multirel_subset,
    Default=[
        AVG,
        COUNT,
        MAX,
        MIN,
        SUM,
    ],
    Minimal=[AVG, COUNT, SUM],
)
"""
Set of default aggregations for [`Multirel`][getml.feature_learning.Multirel].
`All` contains all aggregations
supported by Multirel, `Default` contains the subset of reasonable default aggregations,
`Minimal` is minimal set.
"""

# --------------------------------------------------------------------
