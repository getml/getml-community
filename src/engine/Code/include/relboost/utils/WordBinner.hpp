#ifndef RELBOOST_UTILS_WORDBINNER_HPP_
#define RELBOOST_UTILS_WORDBINNER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetValueType>
using WordBinner = binning::WordBinner<containers::Match, GetValueType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

#endif  // RELBOOST_UTILS_WORDBINNER_HPP_
