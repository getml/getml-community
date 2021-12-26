#ifndef RELBOOST_UTILS_DISCRETEBINNER_HPP_
#define RELBOOST_UTILS_DISCRETEBINNER_HPP_

// ----------------------------------------------------------------------------

#include "binning/binning.hpp"

// ----------------------------------------------------------------------------

#include "relboost/containers/containers.hpp"

// ----------------------------------------------------------------------------

namespace relboost {
namespace utils {
// ----------------------------------------------------------------------------

template <class GetValueType>
using DiscreteBinner = binning::DiscreteBinner<containers::Match, GetValueType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

#endif  // RELBOOST_UTILS_DISCRETEBINNER_HPP_
