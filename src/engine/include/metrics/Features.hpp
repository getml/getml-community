// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef METRICS_FEATURES_HPP_
#define METRICS_FEATURES_HPP_

#include "helpers/Features.hpp"
#include "metrics/Float.hpp"

#include <vector>

namespace metrics {
using Features = std::vector<helpers::Feature<Float>>;
}  // namespace metrics

#endif  // METRICS_FEATURES_HPP_
