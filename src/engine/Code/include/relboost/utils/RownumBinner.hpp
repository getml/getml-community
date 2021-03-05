#ifndef RELBOOST_UTILS_ROWNUMBINNER_HPP_
#define RELBOOST_UTILS_ROWNUMBINNER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetRownumType>
using RownumBinner = binning::RownumBinner<containers::Match, GetRownumType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

#endif  // RELBOOST_UTILS_ROWNUMBINNER_HPP_
