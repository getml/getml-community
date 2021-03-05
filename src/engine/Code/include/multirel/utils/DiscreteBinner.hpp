#ifndef MULTIREL_UTILS_DISCRETEBINNER_HPP_
#define MULTIREL_UTILS_DISCRETEBINNER_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetValueType>
using DiscreteBinner =
    binning::DiscreteBinner<containers::Match*, GetValueType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel

#endif  // MULTIREL_UTILS_DISCRETEBINNER_HPP_
