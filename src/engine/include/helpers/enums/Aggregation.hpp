// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_ENUMS_AGGREGATION_HPP_
#define HELPERS_ENUMS_AGGREGATION_HPP_

// ----------------------------------------------------------------------------

namespace helpers {
namespace enums {

enum class Aggregation {
  avg,
  avg_time_between,
  count,
  count_above_mean,
  count_below_mean,
  count_distinct,
  count_distinct_over_count,
  count_minus_count_distinct,
  ewma_1s,
  ewma_1m,
  ewma_1h,
  ewma_1d,
  ewma_7d,
  ewma_30d,
  ewma_90d,
  ewma_365d,
  ewma_trend_1s,
  ewma_trend_1m,
  ewma_trend_1h,
  ewma_trend_1d,
  ewma_trend_7d,
  ewma_trend_30d,
  ewma_trend_90d,
  ewma_trend_365d,
  first,
  kurtosis,
  last,
  max,
  median,
  min,
  mode,
  num_max,
  num_min,
  q1,
  q5,
  q10,
  q25,
  q75,
  q90,
  q95,
  q99,
  skew,
  stddev,
  sum,
  time_since_first_maximum,
  time_since_first_minimum,
  time_since_last_maximum,
  time_since_last_minimum,
  trend,
  var,
  variation_coefficient
};

}  // namespace enums
}  // namespace helpers

// ----------------------------------------------------------------------------

#endif  // HELPERS_ENUMS_AGGREGATION_HPP_
