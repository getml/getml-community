#ifndef MULTIREL_UTILS_ROWNUMBINNER_HPP_
#define MULTIREL_UTILS_ROWNUMBINNER_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetRownumType>
using RownumBinner = binning::RownumBinner<containers::Match*, GetRownumType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel

#endif  // MULTIREL_UTILS_ROWNUMBINNER_HPP_
