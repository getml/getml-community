# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
This module contains all possible aggregations to be used with
:class:`~getml.feature_learning.Multirel`,
:class:`~getml.feature_learning.FastProp`,
:class:`~getml.preprocessors.Mapping`.


Refer to the :ref:`feature learning section in the user guide
<feature_learning_design>` for details about how
these aggregations are used in the context of feature learning.
"""

from collections import namedtuple
from typing import Literal

Aggregation = Literal[
    "VARIATION COEFFICIENT",
    "VAR",
    "TREND",
    "TIME SINCE LAST MINIMUM",
    "TIME SINCE LAST MAXIMUM",
    "TIME SINCE FIRST MINIMUM",
    "TIME SINCE FIRST MAXIMUM",
    "SUM",
    "STDDEV",
    "SKEW",
    "Q99",
    "Q95",
    "Q90",
    "Q75",
    "Q25",
    "Q10",
    "Q5",
    "Q1",
    "NUM MIN",
    "NUM MAX",
    "MODE",
    "MIN",
    "MEDIAN",
    "MAX",
    "LAST",
    "KURTOSIS",
    "FIRST",
    "EWMA_365D",
    "EWMA_90D",
    "EWMA_30D",
    "EWMA_7D",
    "EWMA_1D",
    "EWMA_1H",
    "EWMA_1M",
    "EWMA_1S",
    "COUNT MINUS COUNT DISTINCT",
    "COUNT DISTINCT OVER COUNT",
    "COUNT DISTINCT",
    "COUNT BELOW MEAN",
    "COUNT ABOVE MEAN",
    "COUNT",
    "AVG",
]

# ------------------------------------------------------------------------------

Avg: Aggregation = "AVG"
"""
Average value of a given numerical column.
"""

Count: Aggregation = "COUNT"
"""
Number of rows in a given column.
"""

CountAboveMean: Aggregation = "COUNT ABOVE MEAN"
"""
Counts the number of values strictly greater than the mean.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

CountBelowMean: Aggregation = "COUNT BELOW MEAN"
"""
Counts the number of values strictly smaller than the mean.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

CountDistinct: Aggregation = "COUNT DISTINCT"
"""
Count function with distinct clause. This only counts unique elements."""

CountDistinctOverCount: Aggregation = "COUNT DISTINCT OVER COUNT"
"""
COUNT DISTINCT divided by COUNT.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

CountMinusCountDistinct: Aggregation = "COUNT MINUS COUNT DISTINCT"
"""
Counts minus counts distinct. Substracts COUNT DISTINCT from COUNT.
"""

EWMA_1s: Aggregation = "EWMA_1S"
"""
Exponentially weighted moving average with a half-life of 1 second.
"""

EWMA_1m: Aggregation = "EWMA_1M"
"""
Exponentially weighted moving average with a half-life of 1 minute.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

EWMA_1h: Aggregation = "EWMA_1H"
"""
Exponentially weighted moving average with a half-life of 1 hour.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

EWMA_1d: Aggregation = "EWMA_1D"
"""
Exponentially weighted moving average with a half-life of 1 day.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

EWMA_7d: Aggregation = "EWMA_7D"
"""
Exponentially weighted moving average with a half-life of 7 days.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

EWMA_30d: Aggregation = "EWMA_30D"
"""
Exponentially weighted moving average with a half-life of 30 days.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

EWMA_90d: Aggregation = "EWMA_90D"
"""
Exponentially weighted moving average with a half-life of 90 days.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

EWMA_365d: Aggregation = "EWMA_365D"
"""
Exponentially weighted moving average with a half-life of 365 days.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""


First: Aggregation = "FIRST"
"""
First value of a given column, when ordered by the time stamp.
"""

Kurtosis: Aggregation = "KURTOSIS"
"""
The kurtosis of a given column.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

Last: Aggregation = "LAST"
"""
Last value of a given column, when ordered by the time stamp.
"""

Max: Aggregation = "MAX"
"""
Largest value of a given column.
"""

Median: Aggregation = "MEDIAN"
"""
Median of a given column
"""

Min: Aggregation = "MIN"
"""
Smallest value of a given column.
"""

Mode: Aggregation = "MODE"
"""
Most frequent value of a given column.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

NumMax: Aggregation = "NUM MAX"
"""
The number of times we observe the maximum value.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

NumMin: Aggregation = "NUM MIN"
"""
The number of times we observe the minimum value.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

Q1: Aggregation = "Q1"
"""
The 1%-quantile.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

Q5: Aggregation = "Q5"
"""
The 5%-quantile.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

Q10: Aggregation = "Q10"
"""
The 10%-quantile.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

Q25: Aggregation = "Q25"
"""
The 25%-quantile.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

Q75: Aggregation = "Q75"
"""
The 75%-quantile.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

Q90: Aggregation = "Q90"
"""
The 90%-quantile.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

Q95: Aggregation = "Q95"
"""
The 95%-quantile.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

Q99: Aggregation = "Q99"
"""
The 99%-quantile.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

Skew: Aggregation = "SKEW"
"""
Skewness of a given column.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

Stddev: Aggregation = "STDDEV"
"""
Standard deviation of a given column.
"""

Sum: Aggregation = "SUM"
"""
Total sum of a given numerical column.
"""

TimeSinceFirstMaximum: Aggregation = "TIME SINCE FIRST MAXIMUM"
"""
The time difference between the first time we see
the maximum value and the time stamp in the population
table. If the maximum value is unique, then TIME SINCE
FIRST MAXIMUM and TIME SINCE LAST MAXIMUM are identical.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

TimeSinceFirstMinimum: Aggregation = "TIME SINCE FIRST MINIMUM"
"""
The time difference between the first time we see
the minimum value and the time stamp in the population
table. If the minimum value is unique, then TIME SINCE
FIRST MINIMUM and TIME SINCE LAST MINIMUM are identical.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

TimeSinceLastMaximum: Aggregation = "TIME SINCE LAST MAXIMUM"
"""
The time difference between the last time we see
the maximum value and the time stamp in the population
table. If the maximum value is unique, then TIME SINCE
FIRST MAXIMUM and TIME SINCE LAST MAXIMUM are identical.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

TimeSinceLastMinimum: Aggregation = "TIME SINCE LAST MINIMUM"
"""
The time difference between the last time we see
the minimum value and the time stamp in the population
table. If the minimum value is unique, then TIME SINCE
FIRST MINIMUM and TIME SINCE LAST MINIMUM are identical.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

Trend: Aggregation = "TREND"
"""
Extracts a linear trend from a variable over time and
extrapolates this trend to the current time stamp.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

Var: Aggregation = "VAR"
"""
Statistical variance of a given numerical column.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

VariationCoefficient: Aggregation = "VARIATION COEFFICIENT"
"""
VAR divided by MEAN.
Please note that this aggregation is
not supported by
:class:`~getml.feature_learning.Multirel`.
"""

# ------------------------------------------------------------------------------

_multirel_subset = [
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


# ------------------------------------------------------------------------------

_additional_aggregations_for_fast_prop = [
    CountAboveMean,
    CountBelowMean,
    CountDistinctOverCount,
    EWMA_1s,
    EWMA_1m,
    EWMA_1h,
    EWMA_1d,
    EWMA_7d,
    EWMA_30d,
    EWMA_90d,
    EWMA_365d,
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

# ------------------------------------------------------------------------------

_mapping_subset = [
    Avg,
    Count,
    CountAboveMean,
    CountBelowMean,
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

# ------------------------------------------------------------------------------

_all_aggregations = _multirel_subset + _additional_aggregations_for_fast_prop

# ------------------------------------------------------------------------------

_Aggregations = namedtuple("_Aggregations", ["All", "Default", "Minimal"])

# ------------------------------------------------------------------------------

fastprop = _Aggregations(
    All=_all_aggregations,
    Default=[
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
    ],
    Minimal=[Avg, Count, Max, Min, Sum],
)
"""
Set of default aggregations for :class:`~getml.feature_learning.FastProp` and
:class:`~getml.feature_learning.FastPropTimeseries`. `All` contains all aggregations
supported by FastProp, `Default` contains the subset of reasonable default aggregations,
`Minimal` is minimal set.
"""

# ------------------------------------------------------------------------------

mapping = _Aggregations(
    All=_mapping_subset,
    Default=[Avg],
    Minimal=[Avg],
)
"""
Set of default aggregations for :class:`~getml.preprocessor.Mapping`. `All` contains all
aggregations supported by the mapping preprocessor. `Default` and `Minimal` are identical
and include only the AVG aggregation, which is the recommended setting for classification
problems.
"""

# ------------------------------------------------------------------------------

multirel = _Aggregations(
    All=_multirel_subset,
    Default=[
        Avg,
        Count,
        Max,
        Min,
        Sum,
    ],
    Minimal=[Avg, Count, Sum],
)
"""
Set of default aggregations for :class:`~getml.feature_learning.Multirel`.
`All` contains all aggregations
supported by Multirel, `Default` contains the subset of reasonable default aggregations,
`Minimal` is minimal set.
"""

# --------------------------------------------------------------------
