#ifndef RELMT_CONTAINERS_PREDICTIONS_HPP_
#define RELMT_CONTAINERS_PREDICTIONS_HPP_

#include <memory>
#include <vector>

#include "relboost/Float.hpp"

namespace relmt {
namespace containers {
// ----------------------------------------------------------------------------

typedef std::vector<std::shared_ptr<const std::vector<Float>>> Predictions;

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace relmt

#endif  // RELMT_CONTAINERS_PREDICTIONS_HPP_
