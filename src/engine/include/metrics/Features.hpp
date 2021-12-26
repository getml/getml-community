#ifndef METRICS_FEATURES_HPP_
#define METRICS_FEATURES_HPP_

// ----------------------------------------------------------------------------

#include <vector>

// ----------------------------------------------------------------------------

#include "helpers/helpers.hpp"

// ----------------------------------------------------------------------------

#include "metrics/Float.hpp"

// ----------------------------------------------------------------------------

namespace metrics {
typedef std::vector<helpers::Feature<Float>> Features;
}  // namespace metrics

// ----------------------------------------------------------------------------

#endif  // METRICS_FEATURES_HPP_
