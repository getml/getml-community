#ifndef HELPERS_MAPPINGAGGREGATION_HPP_
#define HELPERS_MAPPINGAGGREGATION_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

enum class MappingAggregation
{
    avg,
    count,
    count_above_mean,
    count_below_mean,
    count_distinct,
    count_distinct_over_count,
    count_minus_count_distinct,
    kurtosis,
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
    var,
    variation_coefficient
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_MAPPINGAGGREGATION_HPP_
