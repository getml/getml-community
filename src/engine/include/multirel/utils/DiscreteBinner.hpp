#ifndef MULTIREL_UTILS_DISCRETEBINNER_HPP_
#define MULTIREL_UTILS_DISCRETEBINNER_HPP_

// ----------------------------------------------------------------------------
#include "binning/binning.hpp"

// ----------------------------------------------------------------------------

#include "multirel/containers/containers.hpp"

// ----------------------------------------------------------------------------

namespace multirel {
namespace utils {

template <class GetValueType>
using DiscreteBinner =
    binning::DiscreteBinner<containers::Match*, GetValueType>;

}  // namespace utils
}  // namespace multirel

#endif  // MULTIREL_UTILS_DISCRETEBINNER_HPP_
