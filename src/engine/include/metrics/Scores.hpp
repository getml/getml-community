// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef METRICS_SCORES_HPP_
#define METRICS_SCORES_HPP_

#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/define_named_tuple.hpp"
#include "helpers/ColumnDescription.hpp"
#include "metrics/Features.hpp"
#include "metrics/Float.hpp"
#include "metrics/Int.hpp"

namespace metrics {

/// Scores are measure by which the predictive performance of the model is
/// evaluated.
class Scores {
 public:
  /// Measures the overall accuracy.
  using f_accuracy = fct::Field<"accuracy_", std::vector<Float>>;

  /// Measures the area under the (ROC) curve, AUC.
  using f_auc = fct::Field<"auc_", std::vector<Float>>;

  /// Measures the cross entropy loss or log loss.
  using f_cross_entropy = fct::Field<"cross_entropy_", std::vector<Float>>;

  /// Measures the mean absolute error.
  using f_mae = fct::Field<"mae_", std::vector<Float>>;

  /// Measures the root mean squared error.
  using f_rmse = fct::Field<"rmse_", std::vector<Float>>;

  /// Measures the (predictive) R-squared between predictions and targets.
  using f_rsquared = fct::Field<"rsquared_", std::vector<Float>>;

  /// These metrics are used for classification problems.
  using ClassificationMetricsType =
      fct::NamedTuple<f_accuracy, f_auc, f_cross_entropy>;

  /// These metrics are used for regression problems.
  using RegressionMetricsType = fct::NamedTuple<f_mae, f_rmse, f_rsquared>;

  /// Combination of all metrics.
  using AllMetricsType = fct::define_named_tuple_t<ClassificationMetricsType,
                                                   RegressionMetricsType>;

  /// Sometimes we want to preserve the history - at the moment, we only
  /// preserve the metrics, but that might change in the future.
  using HistoryType =
      fct::define_named_tuple_t<fct::Field<"date_time_", std::string>,
                                fct::Field<"set_used_", std::string>,
                                AllMetricsType>;

  /// The accuracy curves feeding the accuracy scores.
  using f_accuracy_curves =
      fct::Field<"accuracy_curves_", std::vector<std::vector<Float>>>;

  /// Average of targets w.r.t. different bins of the feature.
  using f_average_targets =
      fct::Field<"average_targets_",
                 std::vector<std::vector<std::vector<Float>>>>;

  /// The column descriptions, correspond to the column importances.
  using f_column_descriptions =
      fct::Field<"column_descriptions_",
                 std::vector<helpers::ColumnDescription>>;

  /// Importances of individual column w.r.t. targets.
  using f_column_importances =
      fct::Field<"column_importances_", std::vector<std::vector<Float>>>;

  /// Correlations coefficients of features with targets.
  using f_feature_correlations =
      fct::Field<"feature_correlations_", std::vector<std::vector<Float>>>;

  /// Densities of the features.
  using f_feature_densities =
      fct::Field<"feature_densities_", std::vector<std::vector<Int>>>;

  /// Importances of individual features w.r.t. targets.
  using f_feature_importances =
      fct::Field<"feature_importances_", std::vector<std::vector<Float>>>;

  /// The names of the features.
  using f_feature_names =
      fct::Field<"feature_names_", std::vector<std::string>>;

  /// False positive rate.
  using f_fpr = fct::Field<"fpr_", std::vector<std::vector<Float>>>;

  /// The history of the scores.
  using f_history = fct::Field<"history_", std::vector<HistoryType>>;

  /// Min, max and step_size for feature_densities and average targets.
  using f_labels = fct::Field<"labels_", std::vector<std::vector<Float>>>;

  /// The lift (for classification problems)
  using f_lift = fct::Field<"lift_", std::vector<std::vector<Float>>>;

  /// The precision (for classification problems)
  using f_precision = fct::Field<"precision_", std::vector<std::vector<Float>>>;

  /// Minimum prediction - needed for plotting the accuracy.
  using f_prediction_min = fct::Field<"prediction_min_", std::vector<Float>>;

  /// Stepsize - needed for plotting the accuracy.
  using f_prediction_step_size =
      fct::Field<"prediction_size_", std::vector<Float>>;

  /// Proportion of samples called (for the lift curve)
  using f_proportion =
      fct::Field<"proportion_", std::vector<std::vector<Float>>>;

  /// Marks the training/testing/validation set (or any other set defined by the
  /// user).
  using f_set_used = fct::Field<"set_used_", std::string>;

  /// True positive rate.
  using f_tpr = fct::Field<"tpr_", std::vector<std::vector<Float>>>;

  /// This type needs to be defined for the reflection to work.
  using NamedTupleType = fct::define_named_tuple_t<
      AllMetricsType, f_accuracy_curves, f_average_targets,
      f_column_descriptions, f_column_importances, f_feature_correlations,
      f_feature_densities, f_feature_importances, f_feature_names, f_fpr,
      f_history, f_labels, f_lift, f_precision, f_prediction_min,
      f_prediction_step_size, f_proportion, f_set_used, f_tpr>;

 public:
  Scores();

  explicit Scores(const NamedTupleType& _val);

  ~Scores();

 public:
  /// Saves the scores to a JSON file.
  void save(const std::string& _fname) const;

  /// Stores the current state of the metrics in the history
  void to_history();

 public:
  /// Trivial accessor
  const std::vector<std::vector<Float>>& column_importances() const {
    return val_.get<f_column_importances>();
  }

  /// Trivial accessor
  const std::vector<helpers::ColumnDescription>& column_descriptions() const {
    return val_.get<f_column_descriptions>();
  }

  /// Trivial accessor
  const std::vector<std::vector<Float>>& feature_correlations() const {
    return val_.get<f_feature_correlations>();
  }

  /// Trivial accessor
  const std::vector<std::vector<Float>>& feature_importances() const {
    return val_.get<f_feature_importances>();
  }

  /// Trivial accessor
  const std::vector<std::string>& feature_names() const {
    return val_.get<f_feature_names>();
  }

  /// Trivial (const) accessor.
  const auto& fpr(const size_t _target_num) const {
    return get_by_target_num<f_fpr>(_target_num);
  }

  /// Trivial accessor
  const std::vector<HistoryType>& history() const {
    return val_.get<f_history>();
  }

  /// Trivial (const) accessor.
  const auto& lift(const size_t _target_num) const {
    return get_by_target_num<f_lift>(_target_num);
  }

  /// Retrieves the metrics.
  const AllMetricsType metrics() const { return AllMetricsType(val_); }

  /// Necessary for the reflection to work.
  const auto& named_tuple() const { return val_; }

  /// Trivial (const) accessor.
  const auto& precision(const size_t _target_num) const {
    return get_by_target_num<f_precision>(_target_num);
  }

  /// Trivial (const) accessor.
  const auto& proportion(const size_t _target_num) const {
    return get_by_target_num<f_proportion>(_target_num);
  }

  /// Trivial (const) accessor.
  const std::string& set_used() const { return val_.get<f_set_used>(); }

  /// Trivial (const) accessor.
  const auto& tpr(const size_t _target_num) const {
    return get_by_target_num<f_tpr>(_target_num);
  }

  /// Updates the scores object with new fields.
  template <class... FieldType>
  void update(const FieldType&... _fields) {
    val_ = val_.replace(_fields...);
  }

 private:
  /// Helper function to retrieve a vector by the target_num.
  template <class FieldType>
  const std::vector<Float>& get_by_target_num(const size_t _target_num) const {
    const auto& vec = fct::get<FieldType>(val_);
    if (_target_num >= vec.size()) {
      throw std::runtime_error("target_num out of bounds for field '" +
                               FieldType::name_.str() +
                               "', got: " + std::to_string(_target_num) +
                               ", size: " + std::to_string(vec.size()) + ".");
    }
    return vec.at(_target_num);
  }

 private:
  /// The underlying NamedTuple
  NamedTupleType val_;
};

}  // namespace metrics

#endif  // METRICS_SCORES_HPP_
