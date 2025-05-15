// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "optimizers/BFGS.hpp"

namespace optimizers {

BFGS::BFGS(const Float _learning_rate, const size_t _size)
    : B_inv_(Eigen::MatrixXd::Identity(_size, _size)),
      first_iteration_(true),
      learning_rate_(_learning_rate),
      size_(_size) {}

}  // namespace optimizers
