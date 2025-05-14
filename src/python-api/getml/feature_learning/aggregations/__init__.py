# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

from .aggregations import (
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
from .sets import (
    AGGREGATIONS,
    FASTPROP,
    FASTPROP_AGGREGATIONS,
    MAPPING,
    MAPPING_AGGREGATIONS,
    MULTIREL,
    MULTIREL_AGGREGATIONS,
    _all_aggregations,
    _multirel_subset,
)

# Compatibility aliases
Avg = AVG
Count = COUNT
CountDistinct = COUNT_DISTINCT
CountDistinctOverCount = COUNT_DISTINCT_OVER_COUNT
CountMinusCountDistinct = COUNT_MINUS_COUNT_DISTINCT
First = FIRST
Kurtosis = KURTOSIS
Last = LAST
Max = MAX
Median = MEDIAN
Min = MIN
Mode = MODE
NumMax = NUM_MAX
NumMin = NUM_MIN
Skew = SKEW
Stddev = STDDEV
Sum = SUM
TimeSinceFirstMaximum = TIME_SINCE_FIRST_MAXIMUM
TimeSinceFirstMinimum = TIME_SINCE_FIRST_MINIMUM
TimeSinceLastMaximum = TIME_SINCE_LAST_MAXIMUM
TimeSinceLastMinimum = TIME_SINCE_LAST_MINIMUM
Trend = TREND
Var = VAR
VariationCoefficient = VARIATION_COEFFICIENT

__all__ = (
    "FASTPROP",
    "MULTIREL",
    "MAPPING",
    "FASTPROP_AGGREGATIONS",
    "MULTIREL_AGGREGATIONS",
    "MAPPING_AGGREGATIONS",
    "AGGREGATIONS",
    "AVG",
    "COUNT",
    "COUNT_DISTINCT",
    "COUNT_DISTINCT_OVER_COUNT",
    "COUNT_MINUS_COUNT_DISTINCT",
    "EWMA_1S",
    "EWMA_1M",
    "EWMA_1H",
    "EWMA_1D",
    "EWMA_7D",
    "EWMA_30D",
    "EWMA_90D",
    "EWMA_365D",
    "EWMA_TREND_1S",
    "EWMA_TREND_1M",
    "EWMA_TREND_1H",
    "EWMA_TREND_1D",
    "EWMA_TREND_7D",
    "EWMA_TREND_30D",
    "EWMA_TREND_90D",
    "EWMA_TREND_365D",
    "FIRST",
    "KURTOSIS",
    "LAST",
    "MAX",
    "MEDIAN",
    "MIN",
    "MODE",
    "NUM_MAX",
    "NUM_MIN",
    "Q_1",
    "Q_5",
    "Q_10",
    "Q_25",
    "Q_75",
    "Q_90",
    "Q_95",
    "Q_99",
    "SKEW",
    "STDDEV",
    "SUM",
    "TIME_SINCE_FIRST_MAXIMUM",
    "TIME_SINCE_FIRST_MINIMUM",
    "TIME_SINCE_LAST_MAXIMUM",
    "TIME_SINCE_LAST_MINIMUM",
    "TREND",
    "VAR",
    "VARIATION_COEFFICIENT",
    "Avg",
    "Count",
    "CountDistinct",
    "CountDistinctOverCount",
    "CountMinusCountDistinct",
    "First",
    "Kurtosis",
    "Last",
    "Max",
    "Median",
    "Min",
    "Mode",
    "NumMax",
    "NumMin",
    "Skew",
    "Stddev",
    "Sum",
    "TimeSinceFirstMaximum",
    "TimeSinceFirstMinimum",
    "TimeSinceLastMaximum",
    "TimeSinceLastMinimum",
    "Trend",
    "Var",
    "VariationCoefficient",
    "_all_aggregations",
    "_multirel_subset",
)
