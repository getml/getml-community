// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_XGBOOSTPREDICTOR_HPP_
#define PREDICTORS_XGBOOSTPREDICTOR_HPP_

#include <xgboost/c_api.h>

#include <cmath>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "debug/debug.hpp"
#include "predictors/Fingerprint.hpp"
#include "predictors/FloatFeature.hpp"
#include "predictors/IntFeature.hpp"
#include "predictors/Predictor.hpp"
#include "predictors/PredictorImpl.hpp"
#include "predictors/StandardScaler.hpp"
#include "predictors/XGBoostHyperparams.hpp"
#include "predictors/XGBoostMatrix.hpp"

namespace predictors {

/// Implements the XGBoostPredictors
class XGBoostPredictor : public Predictor {
 private:
  typedef std::function<void(BoosterHandle*)> BoosterDestructor;
  typedef XGBoostMatrix::DMatrixDestructor DMatrixDestructor;

  typedef std::unique_ptr<BoosterHandle, BoosterDestructor> BoosterPtr;
  typedef XGBoostMatrix::DMatrixPtr DMatrixPtr;

 public:
  XGBoostPredictor(const XGBoostHyperparams& _hyperparams,
                   const rfl::Ref<const PredictorImpl>& _impl,
                   const std::vector<commands::Fingerprint>& _dependencies)
      : dependencies_(_dependencies),
        hyperparams_(rfl::Ref<XGBoostHyperparams>::make(_hyperparams)),
        impl_(_impl) {}

  ~XGBoostPredictor() = default;

 public:
  /// Returns an importance measure for the individual features
  std::vector<Float> feature_importances(
      const size_t _num_features) const final;

  /// Implements the fit(...) method in scikit-learn style,
  /// but allows us to add an optional validation set.
  std::string fit(
      const std::shared_ptr<const logging::AbstractLogger> _logger,
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical, const FloatFeature& _y,
      const std::optional<std::vector<IntFeature>>& _X_categorical_valid,
      const std::optional<std::vector<FloatFeature>>& _X_numerical_valid,
      const std::optional<FloatFeature>& _y_valid) final;

  /// Loads the predictor
  void load(const std::string& _fname) final;

  /// Implements the predict(...) method in scikit-learn style
  FloatFeature predict(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical) const final;

  /// Saves the predictor
  void save(const std::string& _fname,
            const typename helpers::Saver::Format& _format) const final;

 public:
  /// Whether the predictor accepts null values.
  bool accepts_null() const final { return false; }

  /// Returns a deep copy.
  rfl::Ref<Predictor> clone() const final {
    return rfl::make_ref<XGBoostPredictor>(*this);
  }

  /// Whether the predictor is used for classification;
  bool is_classification() const final {
    const auto objective = hyperparams_->objective();
    using Objective = std::decay_t<decltype(objective)>;
    return objective.value() == Objective::value_of<"reg:logistic">() ||
           objective.value() == Objective::value_of<"binary:logistic">() ||
           objective.value() == Objective::value_of<"binary:logitraw">();
  }

  /// Whether the predictor has been fitted.
  bool is_fitted() const final { return len() > 0; }

  /// Returns the fingerprint of the predictor (necessary to build
  /// the dependency graphs).
  Fingerprint fingerprint() const final {
    using XGBoostFingerprint = typename Fingerprint::XGBoostFingerprint;
    return Fingerprint(XGBoostFingerprint{
        .hyperparams = *hyperparams_,
        .dependencies = dependencies_,
        .other =
            rfl::from_named_tuple<commands::Fingerprint::OtherPredRequirements>(
                impl().reflection())});
  }

  /// Whether we want the predictor to be silent.
  bool silent() const final { return hyperparams_->silent(); }

  /// The type of the predictor.
  std::string type() const final { return "XGBoost"; }

 private:
  /// Frees a Booster pointer
  static void delete_booster(BoosterHandle* _ptr) {
    XGBoosterFree(*_ptr);
    delete _ptr;
  }

  /// Trivial (private) accessor.
  const PredictorImpl& impl() const { return *impl_; }

  /// Returns size of the underlying model
  const bst_ulong len() const { return static_cast<bst_ulong>(model_.size()); }

  /// Returns reference to the underlying model
  const char* model() const {
    assert_true(model_.size() > 0);
    return model_.data();
  }

 private:
  /// Adds a target to _d_matrix.
  void add_target(const DMatrixPtr& _d_matrix, const FloatFeature& _y) const;

  /// Allocates the booster
  BoosterPtr allocate_booster(const DMatrixHandle _dmats[],
                              bst_ulong _len) const;

  /// Convert matrix _mat to a DMatrixHandle
  DMatrixPtr convert_to_in_memory_dmatrix(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical) const;

  /// Convert matrix _mat to a dense DMatrixHandle
  DMatrixPtr convert_to_in_memory_dmatrix_dense(
      const std::vector<FloatFeature>& _X_numerical) const;

  /// Convert matrix _mat to a sparse DMatrixHandle
  DMatrixPtr convert_to_in_memory_dmatrix_sparse(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical) const;

  /// Convert to a memory-mapped matrix.
  XGBoostMatrix convert_to_memory_mapped_dmatrix(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical,
      const std::optional<FloatFeature>& _y) const;

  /// Convert to a dense memory-mapped matrix.
  XGBoostMatrix convert_to_memory_mapped_dmatrix_dense(
      const std::vector<FloatFeature>& _X_numerical,
      const std::optional<FloatFeature>& _y) const;

  /// Convert to a sparse memory-mapped matrix.
  XGBoostMatrix convert_to_memory_mapped_dmatrix_sparse(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical,
      const std::optional<FloatFeature>& _y) const;

  /// Evaluates the current iteration.
  Float evaluate_iter(const DMatrixPtr& _valid_set, const BoosterPtr& _handle,
                      const int _n_iter) const;

  /// Does the actual fitting.
  void fit_handle(const std::shared_ptr<const logging::AbstractLogger> _logger,
                  const XGBoostMatrix& _d_matrix,
                  const std::optional<XGBoostMatrix>& _valid_set,
                  const BoosterPtr& _handle) const;

  /// Generates a matrix for fitting or transformation.
  XGBoostMatrix make_matrix(const std::vector<IntFeature>& _X_categorical,
                            const std::vector<FloatFeature>& _X_numerical,
                            const std::optional<FloatFeature>& _y) const;

  /// Extracts feature importances from XGBoost dump
  void parse_dump(const std::string& _dump,
                  std::vector<Float>* _feature_importances) const;

  /// Sets the hyperparameter for the handle.
  void set_hyperparameters(const BoosterPtr& _handle,
                           const bool _is_memory_mapped) const;

 private:
  /// The dependencies used to build the fingerprint.
  std::vector<commands::Fingerprint> dependencies_;

  /// Hyperparameters for XGBoostPredictor
  const rfl::Ref<const XGBoostHyperparams> hyperparams_;

  /// Implementation class for member functions common to most predictors.
  const rfl::Ref<const PredictorImpl> impl_;

  /// The underlying XGBoost model, expressed in bytes
  std::vector<char> model_;
};

}  // namespace predictors

#endif  // PREDICTORS_XGBOOSTPREDICTOR_HPP_
