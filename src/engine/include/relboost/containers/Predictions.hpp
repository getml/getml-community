#ifndef RELBOOST_CONTAINERS_PREDICTIONS_HPP_
#define RELBOOST_CONTAINERS_PREDICTIONS_HPP_

#include <memory>
#include <vector>

#include "relboost/Float.hpp"

namespace relboost {
namespace containers {

typedef std::vector<std::shared_ptr<const std::vector<Float>>> Predictions;

}  // namespace containers
}  // namespace relboost

#endif  // RELBOOST_CONTAINERS_PREDICTIONS_HPP_
