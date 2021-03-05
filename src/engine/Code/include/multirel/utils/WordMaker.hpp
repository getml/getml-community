#ifndef MULTIREL_UTILS_WORDMAKER_HPP_
#define MULTIREL_UTILS_WORDMAKER_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetRangeType>
using WordMaker = binning::WordMaker<containers::Match*, GetRangeType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel

#endif  // MULTIREL_UTILS_WORDMAKER_HPP_
