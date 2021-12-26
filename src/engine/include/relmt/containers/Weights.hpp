#ifndef RELMT_CONTAINERS_WEIGHTS_HPP_
#define RELMT_CONTAINERS_WEIGHTS_HPP_

// ----------------------------------------------------------------------------

#include <tuple>
#include <vector>

// ----------------------------------------------------------------------------

#include "relboost/Float.hpp"

// ----------------------------------------------------------------------------
namespace relmt {
namespace containers {
// -------------------------------------------------------------------------

typedef typename std::tuple<Float, std::vector<Float>, std::vector<Float>>
    Weights;

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace relmt

// ----------------------------------------------------------------------------

#endif  // RELMT_CONTAINERS_WEIGHTS_HPP_
