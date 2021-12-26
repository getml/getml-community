#ifndef RELMT_UTILS_ROWNUMBINNER_HPP_
#define RELMT_UTILS_ROWNUMBINNER_HPP_

// ------------------------------------------------------------------------

#include "binning/binning.hpp"

// ------------------------------------------------------------------------

#include "relmt/containers/containers.hpp"

// ------------------------------------------------------------------------

namespace relmt {
namespace utils {
// ----------------------------------------------------------------------------

template <class GetRownumType>
using RownumBinner = binning::RownumBinner<containers::Match, GetRownumType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relmt

#endif  // RELMT_UTILS_ROWNUMBINNER_HPP_
