#ifndef RELMT_UTILS_WORDBINNER_HPP_
#define RELMT_UTILS_WORDBINNER_HPP_

// ------------------------------------------------------------------------

#include "binning/binning.hpp"

// ------------------------------------------------------------------------

#include "relmt/containers/containers.hpp"

// ------------------------------------------------------------------------

namespace relmt {
namespace utils {
// ----------------------------------------------------------------------------

template <class GetValueType>
using WordBinner = binning::WordBinner<containers::Match, GetValueType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relmt

#endif  // RELMT_UTILS_WORDBINNER_HPP_
