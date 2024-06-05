// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef METRICS_AUC_HPP_
#define METRICS_AUC_HPP_

#include <cstdint>
#include <utility>
#include <vector>

#include "metrics/Features.hpp"
#include "metrics/Float.hpp"
#include "metrics/MetricImpl.hpp"
#include "metrics/Scores.hpp"
#include "multithreading/multithreading.hpp"
#include <rfl/NamedTuple.hpp>

namespace metrics {

class AUC {
 public:
  using f_auc = typename Scores::f_auc;
  using f_fpr = typename Scores::f_fpr;
  using f_tpr = typename Scores::f_tpr;
  using f_lift = typename Scores::f_lift;
  using f_precision = typename Scores::f_precision;
  using f_proportion = typename Scores::f_proportion;

  using ResultType =
      rfl::NamedTuple<f_auc, f_fpr, f_tpr, f_lift, f_precision, f_proportion>;

 public:
  AUC() {}

  AUC(multithreading::Communicator* _comm) : impl_(_comm) {}

  ~AUC() = default;

 public:
  /// This calculates the loss based on the predictions _yhat
  /// and the targets _y.
  ResultType score(const Features _yhat, const Features _y);

 private:
  /// Calculates the area under the ROC curve.
  Float calc_auc(const std::vector<Float>& _true_positive_rate,
                 const std::vector<Float>& _false_positive_rate) const;

  /// Calculates the absolute number of false positives for different
  /// thresholds.
  std::vector<Float> calc_false_positives(
      const std::vector<Float>& _true_positives,
      const std::vector<Float>& _predicted_negative) const;

  /// Calculates the lift curve.
  std::pair<std::vector<Float>, std::vector<Float>> calc_lift(
      const std::vector<Float>& _true_positive_rate,
      const std::vector<Float>& _predicted_negative,
      const Float _all_negatives) const;

  /// Calculates the precision for different thresholds.
  std::vector<Float> calc_precision(
      const std::vector<Float>& _true_positives,
      const std::vector<Float>& _predicted_negative) const;

  /// Divides every element in _raw by _all.
  std::vector<Float> calc_rate(const std::vector<Float>& _raw,
                               const Float _all) const;

  /// Calculates the number of true positives, before "compression"
  /// (summarizing values for which the prediction is the same)
  std::pair<std::vector<Float>, Float> calc_true_positives_uncompressed(
      const std::vector<std::pair<Float, Float>>& _pairs) const;

  /// "Compresses" _true_postives and _all_positives, meaning that we
  /// summarize values where the prediction is the same. This is necessary,
  /// because there is no clear order to such values.
  std::pair<std::vector<Float>, std::vector<Float>> compress(
      const std::vector<std::pair<Float, Float>>& _pairs,
      const std::vector<Float>& _true_positives_uncompressed,
      const Float _all_positives) const;

  /// Downsamples _original to 200 values or less.
  std::vector<Float> downsample(const std::vector<Float>& _original) const;

  /// Finds the minimum and maximum of a vector.
  std::pair<Float, Float> find_min_max(const size_t _j) const;

  /// Generates the default values when there is no meaningful prediction.
  void make_default_values(std::vector<std::vector<Float>>* _true_positive_arr,
                           std::vector<std::vector<Float>>* _false_positive_arr,
                           std::vector<std::vector<Float>>* _lift_arr,
                           std::vector<std::vector<Float>>* _precision_arr,
                           std::vector<std::vector<Float>>* _proportion_arr,
                           Float* _auc) const;

  /// Generates a vector of prediction-target-pairs.
  std::vector<std::pair<Float, Float>> make_pairs(const size_t _j) const;

 private:
  /// Trivial getter
  multithreading::Communicator& comm() { return impl_.comm(); }

  /// Trivial getter
  size_t ncols() const { return impl_.ncols(); }

  /// Trivial getter
  size_t nrows() const { return impl_.nrows(); }

  /// Trivial getter
  Float yhat(size_t _i, size_t _j) const { return impl_.yhat(_i, _j); }

  /// Trivial getter
  Float y(size_t _i, size_t _j) const { return impl_.y(_i, _j); }

 private:
  /// Contains all the relevant data.
  MetricImpl impl_;
};

}  // namespace metrics

#endif  // METRICS_AUC_HPP_
