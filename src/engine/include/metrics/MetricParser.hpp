// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

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
