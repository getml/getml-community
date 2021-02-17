#ifndef DFS_ENUMS_AGGREGATION_HPP_
#define DFS_ENUMS_AGGREGATION_HPP_

// ----------------------------------------------------------------------------

namespace dfs
{
namespace enums
{
// ------------------------------------------------------------------------

enum class Aggregation
{
    avg,
    avg_time_between,
    count,
    count_distinct,
    count_minus_count_distinct,
    first,
    last,
    max,
    median,
    min,
    skew,
    stddev,
    sum,
    var
};

// ----------------------------------------------------------------------------

}  // namespace enums
}  // namespace dfs

// ----------------------------------------------------------------------------

#endif  // DFS_ENUMS_AGGREGATION_HPP_
