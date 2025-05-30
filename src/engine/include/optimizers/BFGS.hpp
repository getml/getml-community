// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef OPTIMIZERS_BFGS_HPP_
#define OPTIMIZERS_BFGS_HPP_

#include "debug/assert_true.hpp"
#include "optimizers/Float.hpp"
#include "optimizers/Optimizer.hpp"

#include <Eigen/Dense>
#include <cmath>
#include <vector>

namespace optimizers {

// ----------------------------------------------------------------------------

class BFGS final : public Optimizer {
 public:
  BFGS(const Float _learning_rate, const size_t _size);

  ~BFGS() final = default;

  // ----------------------------------------------------------------------------

  /// This member functions implements the updating strategy of the optimizer.
  void update_weights(const Float _epoch_num,
                      const std::vector<Float>& _gradients,
                      std::vector<Float>* _weights) final {
    // --------------------------------------------------------------------

    assert_true(_gradients.size() == _weights->size());
    assert_true(_gradients.size() == size_);

    // --------------------------------------------------------------------

    Eigen::VectorXd g(size_);

    for (size_t i = 0; i < size_; ++i) {
      g(i, 0) = _gradients[i];
    }

    // --------------------------------------------------------------------

    if (!first_iteration_) {
      Eigen::VectorXd y = g - g_old_;

      const Float sTy = s_old_.dot(y);

      B_inv_ =
          B_inv_ +
          (sTy + y.dot(B_inv_ * y)) * s_old_ * s_old_.transpose() /
              (sTy * sTy) -
          (B_inv_ * y * s_old_.transpose() + s_old_ * y.transpose() * B_inv_) /
              sTy;
    }

    // --------------------------------------------------------------------

    Eigen::MatrixXd p = B_inv_ * g * -1.0;
    Eigen::MatrixXd s = p * learning_rate_;

    // --------------------------------------------------------------------

    for (size_t i = 0; i < size_; ++i) {
      if (!std::isnan(s(i, 0)) && !std::isinf(s(i, 0))) {
        (*_weights)[i] += s(i, 0);
      }
    }

    // --------------------------------------------------------------------

    g_old_ = g;

    s_old_ = s;

    first_iteration_ = false;

    // --------------------------------------------------------------------
  }

  // ----------------------------------------------------------------------------

 private:
  /// Estimate of the inverted Hessian matrix.
  Eigen::MatrixXd B_inv_;

  /// Whether this is the first iteration.
  bool first_iteration_;

  /// Gradient in the last iteration.
  Eigen::VectorXd g_old_;

  /// The learning rate used for the AdaGrad algorithm.
  const Float learning_rate_;

  /// The size of the problem.
  const size_t size_;

  /// The value s in the last iteration.
  Eigen::VectorXd s_old_;
};

// ----------------------------------------------------------------------------
}  // namespace optimizers

#endif  // OPTIMIZERS_BFGS_HPP_
