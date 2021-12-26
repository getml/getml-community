#ifndef RELMT_UTILS_CATEGORICALBINNER_HPP_
#define RELMT_UTILS_CATEGORICALBINNER_HPP_

// ----------------------------------------------------------------------------

#include "binning/binning.hpp"

// ----------------------------------------------------------------------------

#include "relmt/containers/containers.hpp"

// ----------------------------------------------------------------------------

namespace relmt {
namespace utils {
// ----------------------------------------------------------------------------

template <class GetValueType>
using CategoricalBinner =
    binning::CategoricalBinner<containers::Match, GetValueType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relmt

#endif  // RELMT_UTILS_CATEGORICALBINNER_HPP_
