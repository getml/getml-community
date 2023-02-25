// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_STANDARDSCALER_HPP_
#define PREDICTORS_STANDARDSCALER_HPP_

#include <memory>
#include <vector>

#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "json/json.hpp"
#include "predictors/CSRMatrix.hpp"
#include "predictors/Float.hpp"
#include "predictors/FloatFeature.hpp"
#include "predictors/Int.hpp"
#include "predictors/IntFeature.hpp"
#include "predictors/PredictorImpl.hpp"

namespace predictors {

/// For rescaling the input data to a standard deviation of one.
class StandardScaler {
 public:
  /// Means of the individual columns.
  using f_mean = fct::Field<"mean_", std::vector<Float>>;

  /// Standard deviations of the individual columns.
  using f_std = fct::Field<"std_", std::vector<Float>>;

  using NamedTupleType = fct::NamedTuple<f_mean, f_std>;

 public:
  StandardScaler() : val_(f_mean({}) * f_std({})){};

  StandardScaler(const NamedTupleType& _val) : val_(_val) {}

  ~StandardScaler() = default;

  /// Calculates the standard deviations for dense data.
  void fit(const std::vector<FloatFeature>& _X_numerical);

  /// Calculates the standard deviations for sparse data.
  void fit(const CSRMatrix<Float, unsigned int, size_t>& _X_sparse);

  /// Transforms dense data.
  std::vector<FloatFeature> transform(
      const std::vector<FloatFeature>& _X_numerical) const;

  /// Transforms sparse data.
  const CSRMatrix<Float, unsigned int, size_t> transform(
      const CSRMatrix<Float, unsigned int, size_t>& _X_sparse) const;

 public:
  /// Necessary for the automated parsing to work.
  const NamedTupleType& named_tuple() const { return val_; }

 private:
  /// Trivial accessor
  inline std::vector<Float>& mean() { return val_.get<f_mean>(); }

  /// Trivial accessor
  inline const std::vector<Float>& mean() const { return val_.get<f_mean>(); }

  /// Trivial accessor
  inline std::vector<Float>& std() { return val_.get<f_std>(); }

  /// Trivial accessor
  inline const std::vector<Float>& std() const { return val_.get<f_std>(); }

 private:
  /// The underlying named tuple.
  NamedTupleType val_;
};

}  // namespace predictors

#endif  // PREDICTORS_STANDARDSCALER_HPP_
