#ifndef RELMT_UTILS_NUMERICALBINNER_HPP_
#define RELMT_UTILS_NUMERICALBINNER_HPP_

// ----------------------------------------------------------------------------

namespace relmt
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetValueType>
using NumericalBinner =
    binning::NumericalBinner<containers::Match, GetValueType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relmt

#endif  // RELMT_UTILS_NUMERICALBINNER_HPP_
