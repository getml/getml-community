#ifndef AUTOSQL_DESCRIPTORS_SAMEUNITSCONTAINER_HPP_
#define AUTOSQL_DESCRIPTORS_SAMEUNITSCONTAINER_HPP_

namespace autosql
{
namespace descriptors
{
// ----------------------------------------------------------------------------

typedef std::vector<std::tuple<ColumnToBeAggregated, ColumnToBeAggregated>>
    SameUnitsContainer;

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace autosql

#endif  // AUTOSQL_DESCRIPTORS_SAMEUNITSCONTAINER_HPP_
