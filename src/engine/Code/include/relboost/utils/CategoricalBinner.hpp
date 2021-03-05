#ifndef RELBOOST_UTILS_CATEGORICALBINNER_HPP_
#define RELBOOST_UTILS_CATEGORICALBINNER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetValueType>
using CategoricalBinner =
    binning::CategoricalBinner<containers::Match, GetValueType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

#endif  // RELBOOST_UTILS_CATEGORICALBINNER_HPP_
