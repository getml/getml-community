#ifndef DFS_ENUMS_DATAUSED_HPP_
#define DFS_ENUMS_DATAUSED_HPP_

// ----------------------------------------------------------------------------

namespace dfs
{
namespace enums
{
// ----------------------------------------------------------------------------

enum class DataUsed
{
    categorical,
    discrete,
    not_applicable,
    numerical,
    same_units_categorical,
    same_units_discrete,
    same_units_discrete_ts,
    same_units_numerical,
    same_units_numerical_ts,
    subfeatures
};

// ----------------------------------------------------------------------------

}  // namespace enums
}  // namespace dfs

// ----------------------------------------------------------------------------

#endif  // DFS_ENUMS_DATAUSED_HPP_
