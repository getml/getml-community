// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_ENUMS_AGGREGATION_HPP_
#define HELPERS_ENUMS_AGGREGATION_HPP_

#include "fct/Literal.hpp"

namespace helpers {
namespace enums {

using Aggregation = fct::Literal<
    "AVG", "AVG TIME BETWEEN", "COUNT", "COUNT DISTINCT",
    "COUNT DISTINCT OVER COUNT", "COUNT MINUS COUNT DISTINCT", "EWMA_1S",
    "EWMA_1M", "EWMA_1H", "EWMA_1D", "EWMA_7D", "EWMA_30D", "EWMA_90D",
    "EWMA_365D", "EWMA_TREND_1S", "EWMA_TREND_1M", "EWMA_TREND_1H",
    "EWMA_TREND_1D", "EWMA_TREND_7D", "EWMA_TREND_30D", "EWMA_TREND_90D",
    "EWMA_TREND_365D", "FIRST", "LAST", "KURTOSIS", "MAX", "MEDIAN", "MIN",
    "MODE", "NUM MAX", "NUM MIN", "Q1", "Q5", "Q10", "Q25", "Q75", "Q90", "Q95",
    "Q99", "SKEW", "SUM", "STDDEV", "TIME SINCE FIRST MAXIMUM",
    "TIME SINCE FIRST MINIMUM", "TIME SINCE LAST MAXIMUM",
    "TIME SINCE LAST MINIMUM", "TREND", "VAR", "VARIATION COEFFICIENT">;

}  // namespace enums
}  // namespace helpers

// ----------------------------------------------------------------------------

#endif  // HELPERS_ENUMS_AGGREGATION_HPP_
