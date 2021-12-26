#ifndef PREDICTORS_XGBOOSTPREDICTOR_HPP_
#define PREDICTORS_XGBOOSTPREDICTOR_HPP_

// -----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// -----------------------------------------------------------------------------

#include <memory>
#include <optional>
#include <vector>

// -----------------------------------------------------------------------------

#include <xgboost/c_api.h>

// -----------------------------------------------------------------------------

#include "debug/debug.hpp"

// -----------------------------------------------------------------------------

#include "predictors/FloatFeature.hpp"
#include "predictors/IntFeature.hpp"
#include "predictors/LinearHyperparams.hpp"
#include "predictors/Predictor.hpp"
#include "predictors/PredictorImpl.hpp"
#include "predictors/StandardScaler.hpp"
#include "predictors/XGBoostHyperparams.hpp"

// -----------------------------------------------------------------------------

namespace predictors {

/// Implements the XGBoostPredictors
class XGBoostPredictor : public Predictor {
 private:
  typedef std::function<void(BoosterHandle*)> BoosterDestructor;
  typedef std::function<void(DMatrixHandle*)> DMatrixDestructor;

  typedef std::unique_ptr<BoosterHandle, BoosterDestructor> BoosterPtr;
  typedef std::unique_ptr<DMatrixHandle, DMatrixDestructor> DMatrixPtr;

  // -----------------------------------------

 public:
  XGBoostPredictor(const Poco::JSON::Object& _cmd,
                   const std::shared_ptr<const PredictorImpl>& _impl,
                   const std::vector<Poco::JSON::Object::Ptr>& _dependencies)
      : cmd_(_cmd),
        dependencies_(_dependencies),
        hyperparams_(XGBoostHyperparams(_cmd)),
        impl_(_impl) {}

  ~XGBoostPredictor() = default;

  // -----------------------------------------

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

  /// Returns the fingerprint of the predictor (necessary to build
  /// the dependency graphs).
  Poco::JSON::Object::Ptr fingerprint() const final;

  /// Implements the predict(...) method in scikit-learn style
  FloatFeature predict(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical) const final;

  /// Saves the predictor
  void save(const std::string& _fname) const final;

  // -------------------------------------------------------------------------

 public:
  /// Whether the predictor accepts null values.
  bool accepts_null() const final { return false; }

  /// Returns a deep copy.
  std::shared_ptr<Predictor> clone() const final {
    return std::make_shared<XGBoostPredictor>(*this);
  }

  /// Whether the predictor is used for classification;
  bool is_classification() const final {
    return hyperparams_.objective_ == "reg:logistic" ||
           hyperparams_.objective_ == "binary:logistic" ||
           hyperparams_.objective_ == "binary:logitraw";
  }

  /// Whether the predictor has been fitted.
  bool is_fitted() const final { return len() > 0; }

  /// Whether we want the predictor to be silent.
  bool silent() const final { return hyperparams_.silent_; }

  /// The type of the predictor.
  std::string type() const final { return "XGBoost"; }

  // -----------------------------------------

 private:
  /// Frees a Booster pointer
  static void delete_booster(BoosterHandle* _ptr) {
    XGBoosterFree(*_ptr);
    delete _ptr;
  };

  /// Frees a DMatrixHandle pointer
  static void delete_dmatrix(DMatrixHandle* _ptr) {
    XGDMatrixFree(*_ptr);
    delete _ptr;
  };

  /// Trivial (private) accessor.
  const PredictorImpl& impl() const {
    assert_true(impl_);
    return *impl_;
  }

  /// Returns size of the underlying model
  const bst_ulong len() const { return static_cast<bst_ulong>(model_.size()); }

  /// Returns reference to the underlying model
  const char* model() const {
    assert_true(model_.size() > 0);
    return model_.data();
  }

  // -----------------------------------------

 private:
  /// Adds a target to _d_matrix.
  void add_target(const DMatrixPtr& _d_matrix, const FloatFeature& _y) const;

  /// Allocates the booster
  BoosterPtr allocate_booster(const DMatrixHandle _dmats[],
                              bst_ulong _len) const;

  /// Convert matrix _mat to a DMatrixHandle
  DMatrixPtr convert_to_dmatrix(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical) const;

  /// Convert matrix _mat to a dense DMatrixHandle
  DMatrixPtr convert_to_dmatrix_dense(
      const std::vector<FloatFeature>& _X_numerical) const;

  /// Convert matrix _mat to a sparse DMatrixHandle
  DMatrixPtr convert_to_dmatrix_sparse(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical) const;

  /// Evaluates the current iteration.
  Float evaluate_iter(const DMatrixPtr& _valid_set, const BoosterPtr& _handle,
                      const int _n_iter) const;

  /// Does the actual fitting.
  void fit_handle(const std::shared_ptr<const logging::AbstractLogger> _logger,
                  const DMatrixPtr& _d_matrix,
                  const std::optional<DMatrixPtr>& _valid_set,
                  const BoosterPtr& _handle) const;

  /// Extracts feature importances from XGBoost dump
  void parse_dump(const std::string& _dump,
                  std::vector<Float>* _feature_importances) const;

  /// Sets the hyperparameter for the handle.
  void set_hyperparameters(const BoosterPtr& _handle) const;

  // -----------------------------------------

 private:
  /// The JSON command used to construct this predictor.
  const Poco::JSON::Object cmd_;

  /// The dependencies used to build the fingerprint.
  std::vector<Poco::JSON::Object::Ptr> dependencies_;

  /// Hyperparameters for XGBoostPredictor
  const XGBoostHyperparams hyperparams_;

  /// Implementation class for member functions common to most predictors.
  std::shared_ptr<const PredictorImpl> impl_;

  /// The underlying XGBoost model, expressed in bytes
  std::vector<char> model_;
};

// ------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_XGBOOSTPREDICTOR_HPP_
