// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FASTPROP_CONTAINERS_PREDICTIONS_HPP_
#define FASTPROP_CONTAINERS_PREDICTIONS_HPP_

#include "fastprop/Float.hpp"

#include <vector>

namespace fastprop {
namespace containers {

using Predictions = std::vector<std::vector<Float>>;

}  // namespace containers
}  // namespace fastprop

#endif  // FASTPROP_CONTAINERS_PREDICTIONS_HPP_
