#ifndef RELBOOST_UTILS_ROWNUMBINNER_HPP_
#define RELBOOST_UTILS_ROWNUMBINNER_HPP_

// ------------------------------------------------------------------------

#include "binning/binning.hpp"

// ------------------------------------------------------------------------

#include "relboost/containers/containers.hpp"

// ------------------------------------------------------------------------

namespace relboost {
namespace utils {

template <class GetRownumType>
using RownumBinner = binning::RownumBinner<containers::Match, GetRownumType>;

}  // namespace utils
}  // namespace relboost

#endif  // RELBOOST_UTILS_ROWNUMBINNER_HPP_
