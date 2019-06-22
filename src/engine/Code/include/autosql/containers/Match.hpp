#ifndef AUTOSQL_CONTAINERS_MATCH_HPP_
#define AUTOSQL_CONTAINERS_MATCH_HPP_

namespace autosql
{
namespace containers
{
// ----------------------------------------------------------------------------

struct Match
{
    bool activated;

    AUTOSQL_INT categorical_value;

    size_t ix_x_perip;

    size_t ix_x_popul;

    AUTOSQL_FLOAT numerical_value;
};

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

#endif  // AUTOSQL_CONTAINERS_MATCH_HPP_
