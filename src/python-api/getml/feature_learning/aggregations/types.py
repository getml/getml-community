# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

from typing import Literal, Union

# ------------------------------------------------------------------------------
Avg = Literal["AVG"]
Count = Literal["COUNT"]
CountDistinct = Literal["COUNT DISTINCT"]
CountDistinctOverCount = Literal["COUNT DISTINCT OVER COUNT"]
CountMinusCountDistinct = Literal["COUNT MINUS COUNT DISTINCT"]
EWMA_1s = Literal["EWMA_1S"]
EWMA_1m = Literal["EWMA_1M"]
EWMA_1h = Literal["EWMA_1H"]
EWMA_1d = Literal["EWMA_1D"]
EWMA_7d = Literal["EWMA_7D"]
EWMA_30d = Literal["EWMA_30D"]
EWMA_90d = Literal["EWMA_90D"]
EWMA_365d = Literal["EWMA_365D"]
EWMA_TREND_1s = Literal["EWMA_TREND_1S"]
EWMA_TREND_1m = Literal["EWMA_TREND_1M"]
EWMA_TREND_1h = Literal["EWMA_TREND_1H"]
EWMA_TREND_1d = Literal["EWMA_TREND_1D"]
EWMA_TREND_7d = Literal["EWMA_TREND_7D"]
EWMA_TREND_30d = Literal["EWMA_TREND_30D"]
EWMA_TREND_90d = Literal["EWMA_TREND_90D"]
EWMA_TREND_365d = Literal["EWMA_TREND_365D"]
First = Literal["FIRST"]
Kurtosis = Literal["KURTOSIS"]
Last = Literal["LAST"]
Max = Literal["MAX"]
Median = Literal["MEDIAN"]
Min = Literal["MIN"]
Mode = Literal["MODE"]
NumMax = Literal["NUM MAX"]
NumMin = Literal["NUM MIN"]
Q1 = Literal["Q1"]
Q5 = Literal["Q5"]
Q10 = Literal["Q10"]
Q25 = Literal["Q25"]
Q75 = Literal["Q75"]
Q90 = Literal["Q90"]
Q95 = Literal["Q95"]
Q99 = Literal["Q99"]
Skew = Literal["SKEW"]
Stddev = Literal["STDDEV"]
Sum = Literal["SUM"]
TimeSinceFirstMaximum = Literal["TIME SINCE FIRST MAXIMUM"]
TimeSinceFirstMinimum = Literal["TIME SINCE FIRST MINIMUM"]
TimeSinceLastMaximum = Literal["TIME SINCE LAST MAXIMUM"]
TimeSinceLastMinimum = Literal["TIME SINCE LAST MINIMUM"]
Trend = Literal["TREND"]
Var = Literal["VAR"]
VariationCoefficient = Literal["VARIATION COEFFICIENT"]


MultirelAggregations = Union[
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
"""
Union of all Multirel aggregation types.
"""

# ------------------------------------------------------------------------------

FastPropAggregations = Union[
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
"""
Union of all FastProp aggregation types.
"""

# ------------------------------------------------------------------------------

MappingAggregations = Union[
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
"""
Union of all Mapping aggregation types.
"""

# ------------------------------------------------------------------------------

Aggregations = Union[MultirelAggregations, FastPropAggregations, MappingAggregations]
"""
Union of all possible aggregation types.
"""
