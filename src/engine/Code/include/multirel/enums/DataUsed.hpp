#ifndef MULTIREL_ENUMS_DATAUSED_HPP_
#define MULTIREL_ENUMS_DATAUSED_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace enums
{
// ----------------------------------------------------------------------------

enum class DataUsed
{
    not_applicable,
    same_unit_categorical,
    same_unit_discrete,
    same_unit_discrete_ts,
    same_unit_numerical,
    same_unit_numerical_ts,
    x_perip_categorical,
    x_perip_numerical,
    x_perip_discrete,
    x_perip_text,
    x_popul_categorical,
    x_popul_numerical,
    x_popul_discrete,
    x_popul_text,
    x_subfeature,
    time_stamps_diff,
    time_stamps_window
};

// ----------------------------------------------------------------------------

}  // namespace enums
}  // namespace multirel

// ----------------------------------------------------------------------------

#endif  // MULTIREL_ENUMS_DATAUSED_HPP_
