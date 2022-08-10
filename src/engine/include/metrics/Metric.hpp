// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef METRICS_METRIC_HPP_
#define METRICS_METRIC_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include "metrics/Features.hpp"

// ----------------------------------------------------------------------------

namespace metrics {
// ----------------------------------------------------------------------------

class Metric {
 public:
  Metric() {}

  virtual ~Metric() = default;

  // -----------------------------------------

  /// This calculates the score based on the predictions _yhat
  /// and the targets _y.
  virtual Poco::JSON::Object score(const Features _yhat, const Features _y) = 0;

  // -----------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_METRIC_HPP_
