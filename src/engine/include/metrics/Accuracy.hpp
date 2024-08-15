// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef METRICS_ACCURACY_HPP_
#define METRICS_ACCURACY_HPP_

#include <rfl/NamedTuple.hpp>

#include "metrics/Features.hpp"
#include "metrics/Float.hpp"
#include "metrics/MetricImpl.hpp"
#include "metrics/Scores.hpp"

namespace metrics {

class Accuracy {
 public:
  using f_accuracy = typename Scores::f_accuracy;
  using f_accuracy_curves = typename Scores::f_accuracy_curves;
  using f_prediction_min = typename Scores::f_prediction_min;
  using f_prediction_step_size = typename Scores::f_prediction_step_size;

  using ResultType = rfl::NamedTuple<f_accuracy, f_accuracy_curves,
                                     f_prediction_min, f_prediction_step_size>;

 public:
  Accuracy() {}

  Accuracy(multithreading::Communicator* _comm) : impl_(_comm) {}

  ~Accuracy() = default;

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

#endif  // METRICS_ACCURACY_HPP_
