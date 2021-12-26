#ifndef PREDICTORS_LINEARREGRESSION_HPP_
#define PREDICTORS_LINEARREGRESSION_HPP_

// -----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// -----------------------------------------------------------------------------

#include <memory>
#include <optional>
#include <vector>

// -----------------------------------------------------------------------------

#include "debug/debug.hpp"

// -----------------------------------------------------------------------------

#include "predictors/FloatFeature.hpp"
#include "predictors/IntFeature.hpp"
#include "predictors/LinearHyperparams.hpp"
#include "predictors/Predictor.hpp"
#include "predictors/PredictorImpl.hpp"
#include "predictors/StandardScaler.hpp"

// -----------------------------------------------------------------------------

namespace predictors {
// -----------------------------------------------------------------------------

/// Linear regression predictor.
class LinearRegression : public Predictor {
  // -------------------------------------------------------------------------

 public:
  LinearRegression(const Poco::JSON::Object& _cmd,
                   const std::shared_ptr<const PredictorImpl>& _impl,
                   const std::vector<Poco::JSON::Object::Ptr>& _dependencies)
      : cmd_(_cmd),
        dependencies_(_dependencies),
        hyperparams_(std::make_shared<LinearHyperparams>(_cmd)),
        impl_(_impl){};

  ~LinearRegression() = default;

  // -------------------------------------------------------------------------

 public:
  /// Returns an importance measure for the individual features.
  std::vector<Float> feature_importances(
      const size_t _num_features) const final;

  /// Returns the fingerprint of the predictor (necessary to build
  /// the dependency graphs).
  Poco::JSON::Object::Ptr fingerprint() const final;

  /// Implements the fit(...) method in scikit-learn style
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

  /// Stores the predictor
  void save(const std::string& _fname) const final;

  // -------------------------------------------------------------------------

 public:
  /// Whether the predictor accepts null values.
  bool accepts_null() const final { return false; }

  /// Returns a deep copy.
  std::shared_ptr<Predictor> clone() const final {
    return std::make_shared<LinearRegression>(*this);
  }

  /// Whether the predictor is used for classification;
  bool is_classification() const final { return false; }

  /// Whether the predictor has been fitted.
  bool is_fitted() const final { return weights_.size() > 0; }

  /// Whether we want the predictor to be silent.
  bool silent() const final { return true; }

  /// The type of the predictor.
  std::string type() const final { return "LinearRegression"; }

  // -------------------------------------------------------------------------

 private:
  /// Trivial (private const) accessor.
  const LinearHyperparams& hyperparams() const {
    assert_true(hyperparams_);
    return *hyperparams_;
  }

  // -------------------------------------------------------------------------

 private:
  Poco::JSON::Object load_json_obj(const std::string& _fname) const;

  /// Generates predictions when no categorical columns have been passed.
  FloatFeature predict_dense(
      const std::vector<FloatFeature>& _X_numerical) const;

  /// Generates predictions when at least one categorical column has been
  /// passed.
  FloatFeature predict_sparse(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical) const;

  /// When possible, the linear regression will be fitted
  ///  arithmetically.
  void solve_arithmetically(const std::vector<FloatFeature>& _X_numerical,
                            const FloatFeature& _y);

  /// When necessary, we will use numerical algorithms.
  void solve_numerically(const std::vector<IntFeature>& _X_categorical,
                         const std::vector<FloatFeature>& _X_numerical,
                         const FloatFeature& _y);

  // -------------------------------------------------------------------------

 private:
  /// Calculates the gradients needed for the updates.
  const void calculate_gradients(const size_t _begin, const size_t _end,
                                 const unsigned int* _indices,
                                 const Float* _data, const Float _delta,
                                 std::vector<Float>* _gradients) {
    assert_true(_gradients->size() == weights_.size());

    for (auto ix = _begin; ix < _end; ++ix) {
      assert_true(_indices[ix] < _gradients->size());
      (*_gradients)[_indices[ix]] += _delta * _data[ix];
    }
    _gradients->back() += _delta;
  }

  /// Applies the L2 regularization term for numerical optimization.
  const void calculate_regularization(const Float _bsize_float,
                                      std::vector<Float>* _gradients) {
    if (hyperparams().reg_lambda_ > 0.0) {
      for (size_t i = 0; i < weights_.size(); ++i) {
        (*_gradients)[i] +=
            hyperparams().reg_lambda_ * weights_[i] * _bsize_float;
      }
    }
  }

  /// Trivial (private) accessor.
  const PredictorImpl& impl() const {
    assert_true(impl_);
    return *impl_;
  }

  /// Returns a sparse prediction.
  const Float predict_sparse(const size_t _begin, const size_t _end,
                             const unsigned int* _indices,
                             const Float* _data) const {
    Float yhat = weights_.back();
    for (auto ix = _begin; ix < _end; ++ix) {
      assert_true(_indices[ix] < weights_.size());
      yhat += _data[ix] * weights_[_indices[ix]];
    }
    return yhat;
  }

  // -------------------------------------------------------------------------

 private:
  /// The JSON command used to construct this predictor.
  const Poco::JSON::Object cmd_;

  /// The dependencies used to build the fingerprint.
  std::vector<Poco::JSON::Object::Ptr> dependencies_;

  /// The hyperparameters used for the LinearRegression.
  std::shared_ptr<const LinearHyperparams> hyperparams_;

  /// Implementation class for member functions common to most predictors.
  std::shared_ptr<const PredictorImpl> impl_;

  /// For rescaling the input data such that the standard deviation of each
  /// column is 1.0
  StandardScaler scaler_;

  /// The slopes of the linear regression.
  std::vector<Float> weights_;

  // -------------------------------------------------------------------------
};

// -----------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_LINEARREGRESSION_HPP_
