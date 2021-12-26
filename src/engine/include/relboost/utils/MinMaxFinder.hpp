#ifndef RELBOOST_UTILS_MINMAXFINDER_HPP_
#define RELBOOST_UTILS_MINMAXFINDER_HPP_

// ----------------------------------------------------------------------------

#include "binning/binning.hpp"

// ----------------------------------------------------------------------------

#include "relboost/containers/containers.hpp"

// ----------------------------------------------------------------------------

namespace relboost {
namespace utils {
// ----------------------------------------------------------------------------

template <class GetValueType, class VType>
using MinMaxFinder =
    binning::MinMaxFinder<containers::Match, GetValueType, VType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

#endif  // RELBOOST_UTILS_MINMAXFINDER_HPP_
