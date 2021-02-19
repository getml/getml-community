#ifndef FASTPROP_ENUMS_DATAUSED_HPP_
#define FASTPROP_ENUMS_DATAUSED_HPP_

// ----------------------------------------------------------------------------

namespace fastprop
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
}  // namespace fastprop

// ----------------------------------------------------------------------------

#endif  // FASTPROP_ENUMS_DATAUSED_HPP_
