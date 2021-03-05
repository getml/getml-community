#ifndef MULTIREL_UTILS_WORDBINNER_HPP_
#define MULTIREL_UTILS_WORDBINNER_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetValueType>
using WordBinner = binning::WordBinner<containers::Match*, GetValueType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel

#endif  // MULTIREL_UTILS_WORDBINNER_HPP_
