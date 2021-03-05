#ifndef RELMT_UTILS_WORDMAKER_HPP_
#define RELMT_UTILS_WORDMAKER_HPP_

// ----------------------------------------------------------------------------

namespace relmt
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetRangeType>
using WordMaker = binning::WordMaker<containers::Match, GetRangeType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relmt

#endif  // RELMT_UTILS_WORDMAKER_HPP_
