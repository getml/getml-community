// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef METRICS_SCORES_HPP_
#define METRICS_SCORES_HPP_

#include <Poco/JSON/Object.h>

#include <string>
#include <utility>
#include <vector>

#include "metrics/Features.hpp"
#include "metrics/Float.hpp"
#include "metrics/Int.hpp"

namespace metrics {

/// Scores are measure by which the predictive performance of the model is
/// evaluated.
class Scores {
 public:
  Scores() {}

  explicit Scores(const Poco::JSON::Object& _json_obj) : Scores() {
    from_json_obj(_json_obj);
  }

  ~Scores() = default;

 public:
  /// Parses a JSON object.
  void from_json_obj(const Poco::JSON::Object& _json_obj);

  /// Saves the scores to a JSON file.
  void save(const std::string& _fname) const;

  /// Transforms the Scores into a JSON object.
  Poco::JSON::Object to_json_obj() const;

  /// Stores the current state of the metrics in the history
  void to_history();

 public:
  /// Trivial accessor
  const std::vector<std::vector<Float>>& column_importances() const {
    return column_importances_;
  }

  /// Trivial accessor
  const std::vector<Poco::JSON::Object::Ptr>& column_descriptions() const {
    return column_descriptions_;
  }

  /// Trivial accessor
  const std::vector<std::vector<Float>>& feature_correlations() const {
    return feature_correlations_;
  }

  /// Trivial accessor
  const std::vector<std::vector<Float>>& feature_importances() const {
    return feature_importances_;
  }

  /// Trivial accessor
  const std::vector<std::string>& feature_names() const {
    return feature_names_;
  }

  /// Trivial accessor
  const std::vector<Poco::JSON::Object::Ptr>& history() const {
    return history_;
  }

  /// Trivial (const) accessor.
  const std::string& set_used() const { return set_used_; }

 private:
  /// Trivial accessor
  std::vector<std::vector<Float>>& accuracy_curves() {
    return accuracy_curves_;
  }

  /// Trivial accessor
  const std::vector<std::vector<Float>>& accuracy_curves() const {
    return accuracy_curves_;
  }

  /// Trivial accessor
  std::vector<std::vector<std::vector<Float>>>& average_targets() {
    return average_targets_;
  }

  /// Trivial accessor
  const std::vector<std::vector<std::vector<Float>>>& average_targets() const {
    return average_targets_;
  }

  /// Trivial accessor
  std::vector<std::vector<Float>>& column_importances() {
    return column_importances_;
  }

  /// Trivial accessor
  std::vector<Poco::JSON::Object::Ptr>& column_descriptions() {
    return column_descriptions_;
  }

  /// Trivial accessor
  std::vector<std::vector<Float>>& feature_correlations() {
    return feature_correlations_;
  }

  /// Trivial accessor
  std::vector<std::vector<Int>>& feature_densities() {
    return feature_densities_;
  }

  /// Trivial accessor
  const std::vector<std::vector<Int>>& feature_densities() const {
    return feature_densities_;
  }

  /// Trivial accessor
  std::vector<std::vector<Float>>& feature_importances() {
    return feature_importances_;
  }

  /// Trivial accessor
  std::vector<std::string>& feature_names() { return feature_names_; }

  /// Trivial accessor
  std::vector<std::vector<Float>>& fpr() { return fpr_; }

  /// Trivial accessor
  const std::vector<std::vector<Float>>& fpr() const { return fpr_; }

  /// Trivial accessor
  std::vector<std::vector<Float>>& labels() { return labels_; }

  /// Trivial accessor
  const std::vector<std::vector<Float>>& labels() const { return labels_; }

  /// Trivial accessor
  std::vector<std::vector<Float>>& lift() { return lift_; }

  /// Trivial accessor
  const std::vector<std::vector<Float>>& lift() const { return lift_; }

  /// Trivial accessor
  std::vector<std::vector<Float>>& precision() { return precision_; }

  /// Trivial accessor
  const std::vector<std::vector<Float>>& precision() const {
    return precision_;
  }

  /// Trivial accessor
  std::vector<std::vector<Float>>& proportion() { return proportion_; }

  /// Trivial accessor
  const std::vector<std::vector<Float>>& proportion() const {
    return proportion_;
  }

  /// Trivial accessor
  std::vector<std::vector<Float>>& tpr() { return tpr_; }

  /// Trivial accessor
  const std::vector<std::vector<Float>>& tpr() const { return tpr_; }

  /// If the 1-dimensional array exists, this updated the corresponding
  /// vector.
  template <class T>
  void update_1d_vector(const Poco::JSON::Object& _json_obj,
                        const std::string& _name, std::vector<T>* _vec) const {
    if (_json_obj.has(_name)) {
      *_vec = jsonutils::JSON::array_to_vector<T>(
          jsonutils::JSON::get_array(_json_obj, _name));
    }
  }

  /// If the 1-dimensional array exists, this updated the corresponding
  /// vector.
  void update_1d_vector(const Poco::JSON::Object& _json_obj,
                        const std::string& _name,
                        std::vector<Poco::JSON::Object::Ptr>* _vec) const {
    if (_json_obj.has(_name)) {
      const auto arr = jsonutils::JSON::get_array(_json_obj, _name);

      for (size_t i = 0; i < arr->size(); ++i) {
        _vec->push_back(arr->getObject(i));
      }
    }
  }

  /// If the 2-dimensional array exists, this updated the corresponding
  /// vector.
  template <class T>
  void update_2d_vector(const Poco::JSON::Object& _json_obj,
                        const std::string& _name,
                        std::vector<std::vector<T>>* _vec) const {
    if (!_json_obj.getArray(_name).isNull()) {
      _vec->clear();

      auto arr = jsonutils::JSON::get_array(_json_obj, _name);

      for (size_t i = 0; i < arr->size(); ++i) {
        _vec->push_back(jsonutils::JSON::array_to_vector<T>(
            arr->getArray(static_cast<unsigned int>(i))));
      }
    }
  }

  /// If the 3-dimensional array exists, this updated the corresponding
  /// vector.
  template <class T>
  void update_3d_vector(const Poco::JSON::Object& _json_obj,
                        const std::string& _name,
                        std::vector<std::vector<std::vector<T>>>* _vec) const {
    if (!_json_obj.getArray(_name).isNull()) {
      _vec->clear();

      auto arr = jsonutils::JSON::get_array(_json_obj, _name);

      for (size_t i = 0; i < arr->size(); ++i) {
        auto vec = std::vector<std::vector<T>>(0);

        auto arr2 = arr->getArray(static_cast<unsigned int>(i));

        for (size_t j = 0; j < arr2->size(); ++j) {
          vec.push_back(jsonutils::JSON::array_to_vector<T>(
              arr2->getArray(static_cast<unsigned int>(j))));
        }

        _vec->emplace_back(std::move(vec));
      }
    }
  }

  /// Transforms a 2-dimensional vector to a 2-dimensional array
  template <class T>
  Poco::JSON::Array::Ptr to_2d_array(
      const std::vector<std::vector<T>>& _2d_vec) const {
    auto arr = Poco::JSON::Array::Ptr(new Poco::JSON::Array());

    for (auto& vec : _2d_vec) {
      arr->add(jsonutils::JSON::vector_to_array_ptr(vec));
    }

    return arr;
  }

  /// Transforms a 3-dimensional vector to a 3-dimensional array
  template <class T>
  Poco::JSON::Array::Ptr to_3d_array(
      const std::vector<std::vector<std::vector<T>>>& _3d_vec) const {
    auto arr = Poco::JSON::Array::Ptr(new Poco::JSON::Array());

    for (auto& vec : _3d_vec) {
      arr->add(to_2d_array(vec));
    }

    return arr;
  }

 private:
  /// Accuracy
  std::vector<Float> accuracy_;

  /// The accuracy curves feeding the accuracy scores.
  std::vector<std::vector<Float>> accuracy_curves_;

  /// Area under curve
  std::vector<Float> auc_;

  /// Average of targets w.r.t. different bins of the feature.
  std::vector<std::vector<std::vector<Float>>> average_targets_;

  /// The column descriptions, correspond to the column importances.
  std::vector<Poco::JSON::Object::Ptr> column_descriptions_;

  /// Importances of individual column w.r.t. targets.
  std::vector<std::vector<Float>> column_importances_;

  /// Logarithm of likelihood of predictions
  std::vector<Float> cross_entropy_;

  /// Correlations coefficients of features with targets.
  std::vector<std::vector<Float>> feature_correlations_;

  /// Densities of the features.
  std::vector<std::vector<Int>> feature_densities_;

  /// Importances of individual features w.r.t. targets.
  std::vector<std::vector<Float>> feature_importances_;

  /// The names of the features.
  std::vector<std::string> feature_names_;

  /// False positive rate.
  std::vector<std::vector<Float>> fpr_;

  /// The history of the scores.
  std::vector<Poco::JSON::Object::Ptr> history_;

  /// Min, max and step_size for feature_densities and average targets.
  std::vector<std::vector<Float>> labels_;

  /// The lift (for classification problems)
  std::vector<std::vector<Float>> lift_;

  /// Mean absolute error
  std::vector<Float> mae_;

  /// Precision
  std::vector<std::vector<Float>> precision_;

  /// Minimum prediction - needed for plotting the accuracy.
  std::vector<Float> prediction_min_;

  /// Stepsize - needed for plotting the accuracy.
  std::vector<Float> prediction_step_size_;

  /// Proportion of samples called (for the lift curve)
  std::vector<std::vector<Float>> proportion_;

  /// Root mean squared error
  std::vector<Float> rmse_;

  /// Predictive rsquared
  std::vector<Float> rsquared_;

  /// Marks the training/testing/validation set
  std::string set_used_;

  /// True positive rate.
  std::vector<std::vector<Float>> tpr_;
};

}  // namespace metrics

#endif  // METRICS_SCORES_HPP_
