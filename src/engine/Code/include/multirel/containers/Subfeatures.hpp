#ifndef MULTIREL_CONTAINERS_SUBFEATURES_HPP_
#define MULTIREL_CONTAINERS_SUBFEATURES_HPP_

namespace multirel
{
namespace containers
{
// ----------------------------------------------------------------------------

typedef std::vector<
    ColumnView<Float, std::map<Int, Int>>>
    Subfeatures;

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace multirel

#endif  // MULTIREL_CONTAINERS_SUBFEATURES_HPP_