#ifndef AUTOSQL_SAMPLE_HPP_
#define AUTOSQL_SAMPLE_HPP_

namespace autosql
{
// ----------------------------------------------------------------------------

struct Sample
{
    bool activated;

    AUTOSQL_INT categorical_value;

    size_t ix_x_perip;

    size_t ix_x_popul;

    AUTOSQL_FLOAT numerical_value;
};

// ----------------------------------------------------------------------------
}  // namespace autosql

#endif  // AUTOSQL_SAMPLE_HPP_
