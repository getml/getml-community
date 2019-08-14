#ifndef AUTOSQL_ENUMS_DATAUSED_HPP_
#define AUTOSQL_ENUMS_DATAUSED_HPP_

// ----------------------------------------------------------------------------

namespace autosql
{
namespace enums
{
// ----------------------------------------------------------------------------

enum class DataUsed
{
    not_applicable,
    same_unit_categorical,
    same_unit_discrete,
    same_unit_numerical,
    x_perip_categorical,
    x_perip_numerical,
    x_perip_discrete,
    x_popul_categorical,
    x_popul_numerical,
    x_popul_discrete,
    x_subfeature,
    time_stamps_diff,
    time_stamps_window
};

// ----------------------------------------------------------------------------

}  // namespace enums
}  // namespace autosql

// ----------------------------------------------------------------------------

#endif  // AUTOSQL_ENUMS_DATAUSED_HPP_
