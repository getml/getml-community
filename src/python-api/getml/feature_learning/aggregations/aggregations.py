# Copyright 2025 Code17 GmbH
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


Refer to the [feature learning section][feature-engineering-design-principles] in the user guide for details about how
these aggregations are used in the context of feature learning.
"""

from typing import Final

from getml.feature_learning.aggregations.types import (
    Q1,
    Q5,
    Q10,
    Q25,
    Q75,
    Q90,
    Q95,
    Q99,
    Avg,
    Count,
    CountDistinct,
    CountDistinctOverCount,
    CountMinusCountDistinct,
    EWMA_1d,
    EWMA_1h,
    EWMA_1m,
    EWMA_1s,
    EWMA_7d,
    EWMA_30d,
    EWMA_90d,
    EWMA_365d,
    EWMA_TREND_1d,
    EWMA_TREND_1h,
    EWMA_TREND_1m,
    EWMA_TREND_1s,
    EWMA_TREND_7d,
    EWMA_TREND_30d,
    EWMA_TREND_90d,
    EWMA_TREND_365d,
    First,
    Kurtosis,
    Last,
    Max,
    Median,
    Min,
    Mode,
    NumMax,
    NumMin,
    Skew,
    Stddev,
    Sum,
    TimeSinceFirstMaximum,
    TimeSinceFirstMinimum,
    TimeSinceLastMaximum,
    TimeSinceLastMinimum,
    Trend,
    Var,
    VariationCoefficient,
)

# ------------------------------------------------------------------------------
AVG: Final[Avg] = "AVG"
"""  
Average value of a given numerical column.  
"""

COUNT: Final[Count] = "COUNT"
"""  
Number of rows in a given column.  
"""

COUNT_DISTINCT: Final[CountDistinct] = "COUNT DISTINCT"
"""Count function with distinct clause. This only counts unique elements."""

COUNT_DISTINCT_OVER_COUNT: Final[CountDistinctOverCount] = "COUNT DISTINCT OVER COUNT"
"""COUNT DISTINCT divided by COUNT. Please note that this aggregation is not 
supported by [`Multirel`][getml.feature_learning.Multirel]."""

COUNT_MINUS_COUNT_DISTINCT: Final[CountMinusCountDistinct] = (
    "COUNT MINUS COUNT DISTINCT"
)
"""  
Counts minus counts distinct. Substracts COUNT DISTINCT from COUNT.  
"""

EWMA_1S: Final[EWMA_1s] = "EWMA_1S"
"""  
Exponentially weighted moving average with a half-life of 1 second.  
"""

EWMA_1M: Final[EWMA_1m] = "EWMA_1M"
"""Exponentially weighted moving average with a half-life of 1 minute. Please note 
that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

EWMA_1H: Final[EWMA_1h] = "EWMA_1H"
"""Exponentially weighted moving average with a half-life of 1 hour. Please note that 
this aggregation is not supported by [`Multirel`][getml.feature_learning.Multirel]."""

EWMA_1D: Final[EWMA_1d] = "EWMA_1D"
"""Exponentially weighted moving average with a half-life of 1 day. Please note that 
this aggregation is not supported by [`Multirel`][getml.feature_learning.Multirel]."""

EWMA_7D: Final[EWMA_7d] = "EWMA_7D"
"""Exponentially weighted moving average with a half-life of 7 days. Please note that 
this aggregation is not supported by [`Multirel`][getml.feature_learning.Multirel]."""

EWMA_30D: Final[EWMA_30d] = "EWMA_30D"
"""Exponentially weighted moving average with a half-life of 30 days. Please note 
that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

EWMA_90D: Final[EWMA_90d] = "EWMA_90D"
"""Exponentially weighted moving average with a half-life of 90 days. Please note 
that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

EWMA_365D: Final[EWMA_365d] = "EWMA_365D"
"""Exponentially weighted moving average with a half-life of 365 days. Please note 
that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

EWMA_TREND_1S: Final[EWMA_TREND_1s] = "EWMA_TREND_1S"
"""  
Exponentially weighted trend with a half-life of 1 second.  
"""

EWMA_TREND_1M: Final[EWMA_TREND_1m] = "EWMA_TREND_1M"
"""Exponentially weighted trend with a half-life of 1 minute. Please note that this 
aggregation is not supported by [`Multirel`][getml.feature_learning.Multirel]."""

EWMA_TREND_1H: Final[EWMA_TREND_1h] = "EWMA_TREND_1H"
"""Exponentially weighted trend with a half-life of 1 hour. Please note that this 
aggregation is not supported by [`Multirel`][getml.feature_learning.Multirel]."""

EWMA_TREND_1D: Final[EWMA_TREND_1d] = "EWMA_TREND_1D"
"""Exponentially weighted trend with a half-life of 1 day. Please note that this 
aggregation is not supported by [`Multirel`][getml.feature_learning.Multirel]."""

EWMA_TREND_7D: Final[EWMA_TREND_7d] = "EWMA_TREND_7D"
"""Exponentially weighted trend with a half-life of 7 days. Please note that this 
aggregation is not supported by [`Multirel`][getml.feature_learning.Multirel]."""

EWMA_TREND_30D: Final[EWMA_TREND_30d] = "EWMA_TREND_30D"
"""Exponentially weighted trend with a half-life of 30 days. Please note that this 
aggregation is not supported by [`Multirel`][getml.feature_learning.Multirel]."""

EWMA_TREND_90D: Final[EWMA_TREND_90d] = "EWMA_TREND_90D"
"""Exponentially weighted trend with a half-life of 90 days. Please note that this 
aggregation is not supported by [`Multirel`][getml.feature_learning.Multirel]."""

EWMA_TREND_365D: Final[EWMA_TREND_365d] = "EWMA_TREND_365D"
"""Exponentially weighted trend with a half-life of 365 days. Please note that this 
aggregation is not supported by [`Multirel`][getml.feature_learning.Multirel]."""

FIRST: Final[First] = "FIRST"
"""  
First value of a given column, when ordered by the time stamp.  
"""

KURTOSIS: Final[Kurtosis] = "KURTOSIS"
"""The kurtosis of a given column. Please note that this aggregation is not supported 
by [`Multirel`][getml.feature_learning.Multirel]."""

LAST: Final[Last] = "LAST"
"""Last value of a given column, when ordered by the time stamp."""

MAX: Final[Max] = "MAX"
"""Largest value of a given column."""

MEDIAN: Final[Median] = "MEDIAN"
"""Median of a given column."""

MIN: Final[Min] = "MIN"
"""Smallest value of a given column."""

MODE: Final[Mode] = "MODE"
"""Most frequent value of a given column. Please note that this aggregation is not 
supported by [`Multirel`][getml.feature_learning.Multirel]."""

NUM_MAX: Final[NumMax] = "NUM MAX"
"""The number of times we observe the maximum value. Please note that this 
aggregation is not supported by [`Multirel`][getml.feature_learning.Multirel]."""

NUM_MIN: Final[NumMin] = "NUM MIN"
"""The number of times we observe the minimum value. Please note that this 
aggregation is not supported by [`Multirel`][getml.feature_learning.Multirel]."""

Q_1: Final[Q1] = "Q1"
"""The 1%-quantile. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

Q_5: Final[Q5] = "Q5"
"""The 5%-quantile. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

Q_10: Final[Q10] = "Q10"
"""The 10%-quantile. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

Q_25: Final[Q25] = "Q25"
"""The 25%-quantile. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

Q_75: Final[Q75] = "Q75"
"""The 75%-quantile. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

Q_90: Final[Q90] = "Q90"
"""The 90%-quantile. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

Q_95: Final[Q95] = "Q95"
"""The 95%-quantile. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

Q_99: Final[Q99] = "Q99"
"""The 99%-quantile. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

SKEW: Final[Skew] = "SKEW"
"""Skewness of a given column. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

STDDEV: Final[Stddev] = "STDDEV"
"""Standard deviation of a given column."""

SUM: Final[Sum] = "SUM"
"""Total sum of a given numerical column."""

TIME_SINCE_FIRST_MAXIMUM: Final[TimeSinceFirstMaximum] = "TIME SINCE FIRST MAXIMUM"
"""The time difference between the first time we see the maximum value and the time 
stamp in the population table. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

TIME_SINCE_FIRST_MINIMUM: Final[TimeSinceFirstMinimum] = "TIME SINCE FIRST MINIMUM"
"""The time difference between the first time we see the minimum value and the time 
stamp in the population table. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

TIME_SINCE_LAST_MAXIMUM: Final[TimeSinceLastMaximum] = "TIME SINCE LAST MAXIMUM"
"""The time difference between the last time we see the maximum value and the time 
stamp in the population table. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

TIME_SINCE_LAST_MINIMUM: Final[TimeSinceLastMinimum] = "TIME SINCE LAST MINIMUM"
"""The time difference between the last time we see the minimum value and the time 
stamp in the population table. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

TREND: Final[Trend] = "TREND"
"""Extracts a linear trend from a variable over time and extrapolates this trend to 
the current time stamp. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

VAR: Final[Var] = "VAR"
"""Statistical variance of a given numerical column. Please note that this 
aggregation is not supported by [`Multirel`][getml.feature_learning.Multirel]."""

VARIATION_COEFFICIENT: Final[VariationCoefficient] = "VARIATION COEFFICIENT"
"""VAR divided by MEAN. Please note that this aggregation is not supported by 
[`Multirel`][getml.feature_learning.Multirel]."""

# ------------------------------------------------------------------------------
