#ifndef AUTOSQL_CONTAINERS_SUBFEATURES_HPP_
#define AUTOSQL_CONTAINERS_SUBFEATURES_HPP_

namespace autosql
{
namespace containers
{
// ----------------------------------------------------------------------------

typedef std::vector<
    ColumnView<AUTOSQL_FLOAT, std::map<AUTOSQL_INT, AUTOSQL_INT>>>
    Subfeatures;

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

#endif  // AUTOSQL_CONTAINERS_SUBFEATURES_HPP_