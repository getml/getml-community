// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "optimizers/AdaGrad.hpp"

namespace optimizers {

AdaGrad::AdaGrad(const Float _learning_rate, const Float _offset,
                 const size_t _size)
    : learning_rate_(_learning_rate),
      offset_(_offset),
      sum_squared_gradients_(std::vector<Float>(_size)) {}

}  // namespace optimizers
