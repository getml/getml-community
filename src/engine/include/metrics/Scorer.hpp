// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef METRICS_SCORER_HPP_
#define METRICS_SCORER_HPP_

#include <rfl/define_named_tuple.hpp>
#include <variant>

#include "metrics/AUC.hpp"
#include "metrics/Accuracy.hpp"
#include "metrics/CrossEntropy.hpp"
#include "metrics/Features.hpp"
#include "metrics/MAE.hpp"
#include "metrics/RMSE.hpp"
#include "metrics/RSquared.hpp"

namespace metrics {

struct Scorer {
  using ClassificationMetricsType =
      rfl::define_named_tuple_t<typename AUC::ResultType,
                                typename Accuracy::ResultType,
                                typename CrossEntropy::ResultType>;

  using RegressionMetricsType =
      rfl::define_named_tuple_t<typename MAE::ResultType,
                                typename RMSE::ResultType,
                                typename RSquared::ResultType>;

  using MetricsType =
      std::variant<ClassificationMetricsType, RegressionMetricsType>;

  /// Calculates the basic metrics.
  static MetricsType score(const bool _is_classification, const Features _yhat,
                           const Features _y);
};

}  // namespace metrics

#endif  // METRICS_SCORER_HPP_
