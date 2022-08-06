// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef METRICS_SCORER_HPP_
#define METRICS_SCORER_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include "metrics/Features.hpp"

// ----------------------------------------------------------------------------

namespace metrics {
// ----------------------------------------------------------------------------

struct Scorer {
  /// Returns the metrics in the object.
  static Poco::JSON::Object get_metrics(const Poco::JSON::Object& _obj);

  /// Calculates scores.
  static Poco::JSON::Object score(const bool _is_classification,
                                  const Features _yhat, const Features _y);
};

// ----------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_SCORER_HPP_
