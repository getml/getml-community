#ifndef RELBOOST_UTILS_WORDBINNER_HPP_
#define RELBOOST_UTILS_WORDBINNER_HPP_

// ------------------------------------------------------------------------

#include "binning/binning.hpp"

// ------------------------------------------------------------------------

#include "relboost/containers/containers.hpp"

// ------------------------------------------------------------------------

namespace relboost {
namespace utils {

template <class GetValueType>
using WordBinner = binning::WordBinner<containers::Match, GetValueType>;

}  // namespace utils
}  // namespace relboost

#endif  // RELBOOST_UTILS_WORDBINNER_HPP_
