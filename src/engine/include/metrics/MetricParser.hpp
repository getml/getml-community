#ifndef METRICS_METRICPARSER_HPP_
#define METRICS_METRICPARSER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------

#include "metrics/Features.hpp"
#include "metrics/Metric.hpp"

// ----------------------------------------------------------------------------

namespace metrics {
// ----------------------------------------------------------------------------

struct MetricParser {
  /// Given the _tree, return a shared pointer containing the appropriate
  /// metric.
  static std::shared_ptr<Metric> parse(
      const std::string& _type, multithreading::Communicator* _comm = nullptr);
};

// ----------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_METRICPARSER_HPP_
