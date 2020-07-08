#ifndef RELBOOST_ENUMS_DATAUSED_HPP_
#define RELBOOST_ENUMS_DATAUSED_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace enums
{
// ----------------------------------------------------------------------------

enum class DataUsed
{
    categorical_input,
    categorical_output,
    discrete_input,
    discrete_input_is_nan,
    discrete_output,
    discrete_output_is_nan,
    numerical_input,
    numerical_input_is_nan,
    numerical_output,
    numerical_output_is_nan,
    same_units_categorical,
    same_units_discrete,
    same_units_discrete_is_nan,
    same_units_discrete_ts,
    same_units_numerical,
    same_units_numerical_is_nan,
    same_units_numerical_ts,
    subfeatures,
    time_stamps_diff,
    time_stamps_window
};

// ----------------------------------------------------------------------------

}  // namespace enums
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_ENUMS_DATAUSED_HPP_
