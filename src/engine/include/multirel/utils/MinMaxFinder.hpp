#ifndef MULTIREL_UTILS_MINMAXFINDER_HPP_
#define MULTIREL_UTILS_MINMAXFINDER_HPP_

// ----------------------------------------------------------------------------

#include "binning/binning.hpp"

// ----------------------------------------------------------------------------

#include "multirel/containers/containers.hpp"

// ----------------------------------------------------------------------------

namespace multirel {
namespace utils {

template <class GetValueType, class VType>
using MinMaxFinder =
    binning::MinMaxFinder<containers::Match*, GetValueType, VType>;

}  // namespace utils
}  // namespace multirel

#endif  // MULTIREL_UTILS_MINMAXFINDER_HPP_
