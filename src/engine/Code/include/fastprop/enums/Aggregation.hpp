#ifndef FASTPROP_ENUMS_AGGREGATION_HPP_
#define FASTPROP_ENUMS_AGGREGATION_HPP_

// ----------------------------------------------------------------------------

namespace fastprop
{
namespace enums
{
// ------------------------------------------------------------------------

enum class Aggregation
{
    avg,
    avg_time_between,
    count,
    count_above_mean,
    count_below_mean,
    count_distinct,
    count_minus_count_distinct,
    first,
    kurtosis,
    last,
    max,
    median,
    min,
    mode,
    skew,
    stddev,
    sum,
    var,
    variation_coefficient
};

// ----------------------------------------------------------------------------

}  // namespace enums
}  // namespace fastprop

// ----------------------------------------------------------------------------

#endif  // FASTPROP_ENUMS_AGGREGATION_HPP_
