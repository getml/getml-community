// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "metrics/Accuracy.hpp"

#include <numeric>

namespace metrics {

typename Accuracy::ResultType Accuracy::score(const Features _yhat,
                                              const Features _y) {
  impl_.set_data(_yhat, _y);

  std::vector<Float> accuracy(ncols());

  auto accuracy_curves = std::vector<std::vector<Float>>();

  std::vector<Float> prediction_min(ncols());
  std::vector<Float> prediction_step_size(ncols());

  for (size_t j = 0; j < ncols(); ++j) {
    Float yhat_min = yhat(0, j);

    Float yhat_max = yhat(0, j);

    for (size_t i = 0; i < nrows(); ++i) {
      if (yhat(i, j) < yhat_min)
        yhat_min = yhat(i, j);

      else if (yhat(i, j) > yhat_max)
        yhat_max = yhat(i, j);
    }

    if (impl_.has_comm()) {
      impl_.reduce(multithreading::minimum<Float>(), &yhat_min);

      impl_.reduce(multithreading::maximum<Float>(), &yhat_max);
    }

    const size_t num_critical_values = 200;

    // We use num_critical_values - 1, so that the greatest
    // critical_value will actually be greater than y_max.
    // This is to avoid segfaults.
    const Float step_size =
        (yhat_max - yhat_min) / static_cast<Float>(num_critical_values - 1);

    std::vector<Float> negatives(num_critical_values);

    std::vector<Float> false_positives(num_critical_values);

    for (size_t i = 0; i < nrows(); ++i) {
      if (y(i, j) != 0.0 && y(i, j) != 1.0) {
        throw std::runtime_error(
            "For the accuracy metric, the target "
            "values can only be zero or one! Found "
            "value: " +
            std::to_string(y(i, j)));
      }

      // Note that this operation will always round down.
      const size_t crv =
          static_cast<size_t>((yhat(i, j) - yhat_min) / step_size);

      negatives[crv] += 1.0;

      false_positives[crv] += y(i, j);
    }

    if (impl_.has_comm()) {
      impl_.reduce(std::plus<Float>(), &negatives);

      impl_.reduce(std::plus<Float>(), &false_positives);
    }

    std::partial_sum(negatives.begin(), negatives.end(), negatives.begin());

    std::partial_sum(false_positives.begin(), false_positives.end(),
                     false_positives.begin());

    const auto nrows = negatives.back();

    const auto all_positives = false_positives.back();

    std::vector<Float> accuracies(num_critical_values);

    for (size_t i = 0; i < num_critical_values; ++i) {
      const auto true_positives = all_positives - false_positives[i];

      const auto true_negatives = negatives[i] - false_positives[i];

      accuracies[i] = (true_positives + true_negatives) / nrows;
    }

    accuracy[j] = *std::max_element(accuracies.begin(), accuracies.end());

    prediction_min[j] = yhat_min;
    prediction_step_size[j] = step_size;

    accuracy_curves.push_back(accuracies);
  }

  return f_accuracy(accuracy) * f_accuracy_curves(accuracy_curves) *
         f_prediction_min(prediction_min) *
         f_prediction_step_size(prediction_step_size);
}

}  // namespace metrics
