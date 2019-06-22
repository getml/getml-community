#ifndef AUTOSQL_CONTAINERS_SUBFEATURES_HPP_
#define AUTOSQL_CONTAINERS_SUBFEATURES_HPP_

namespace autosql
{
namespace containers
{
// ----------------------------------------------------------------------------

typedef std::vector<
    ColumnView<Float, std::map<Int, Int>>>
    Subfeatures;

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

#endif  // AUTOSQL_CONTAINERS_SUBFEATURES_HPP_