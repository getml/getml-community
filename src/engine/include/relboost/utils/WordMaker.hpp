#ifndef RELBOOST_UTILS_WORDMAKER_HPP_
#define RELBOOST_UTILS_WORDMAKER_HPP_

// ------------------------------------------------------------------------

#include "binning/binning.hpp"

// ------------------------------------------------------------------------

#include "relboost/containers/containers.hpp"

// ------------------------------------------------------------------------

namespace relboost {
namespace utils {
// ----------------------------------------------------------------------------

template <class GetRangeType>
using WordMaker = binning::WordMaker<containers::Match, GetRangeType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

#endif  // RELBOOST_UTILS_WORDMAKER_HPP_
