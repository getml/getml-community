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
  ewma1s,
  ewma1m,
  ewma1h,
  ewma1d,
  ewma7d,
  ewma30d,
  ewma90d,
  ewma365d,
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
