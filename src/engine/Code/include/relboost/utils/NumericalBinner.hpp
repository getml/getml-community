#ifndef RELBOOST_UTILS_NUMERICALBINNER_HPP_
#define RELBOOST_UTILS_NUMERICALBINNER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetValueType>
using NumericalBinner =
    binning::NumericalBinner<containers::Match, GetValueType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

#endif  // RELBOOST_UTILS_NUMERICALBINNER_HPP_
