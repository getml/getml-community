// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef METRICS_MAE_HPP_
#define METRICS_MAE_HPP_

#include <cstdint>

#include "metrics/Features.hpp"
#include "metrics/Float.hpp"
#include "metrics/MetricImpl.hpp"
#include "multithreading/multithreading.hpp"
#include <rfl/Field.hpp>

namespace metrics {

class MAE {
 public:
  using ResultType = rfl::Field<"mae_", std::vector<Float>>;

 public:
  MAE() {}

  MAE(multithreading::Communicator* _comm) : impl_(_comm) {}

  ~MAE() = default;

  /// This calculates the loss based on the predictions _yhat
  /// and the targets _y.
  ResultType score(const Features _yhat, const Features _y);

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

#endif  // METRICS_MAE_HPP_
