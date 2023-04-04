// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef METRICS_FEATURES_HPP_
#define METRICS_FEATURES_HPP_

#include <vector>

#include "helpers/helpers.hpp"
#include "metrics/Float.hpp"

namespace metrics {
typedef std::vector<helpers::Feature<Float>> Features;
}  // namespace metrics

#endif  // METRICS_FEATURES_HPP_
