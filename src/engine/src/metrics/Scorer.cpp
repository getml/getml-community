// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "metrics/Scorer.hpp"

namespace metrics {

typename Scorer::MetricsType Scorer::score(const bool _is_classification,
                                           const Features _yhat,
                                           const Features _y) {
  if (_is_classification) {
    const auto accuracy = Accuracy().score(_yhat, _y);
    const auto auc = AUC().score(_yhat, _y);
    const auto cross_entropy = CrossEntropy().score(_yhat, _y);

    return ClassificationMetricsType(accuracy * auc * cross_entropy);

  } else {
    const auto mae = MAE().score(_yhat, _y);
    const auto rmse = RMSE().score(_yhat, _y);
    const auto rsquared = RSquared().score(_yhat, _y);

    return RegressionMetricsType(mae * rmse * rsquared);
  }
}

}  // namespace metrics
