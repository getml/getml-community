#ifndef MULTIREL_DESCRIPTORS_SAMEUNITSCONTAINER_HPP_
#define MULTIREL_DESCRIPTORS_SAMEUNITSCONTAINER_HPP_

namespace multirel
{
namespace descriptors
{
// ----------------------------------------------------------------------------

typedef std::vector<std::tuple<ColumnToBeAggregated, ColumnToBeAggregated>>
    SameUnitsContainer;

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace multirel

#endif  // MULTIREL_DESCRIPTORS_SAMEUNITSCONTAINER_HPP_
