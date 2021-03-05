#ifndef RELMT_UTILS_DISCRETEBINNER_HPP_
#define RELMT_UTILS_DISCRETEBINNER_HPP_

// ----------------------------------------------------------------------------

namespace relmt
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetValueType>
using DiscreteBinner = binning::DiscreteBinner<containers::Match, GetValueType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relmt

#endif  // RELMT_UTILS_DISCRETEBINNER_HPP_
