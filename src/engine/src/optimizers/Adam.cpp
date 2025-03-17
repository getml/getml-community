// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "optimizers/Adam.hpp"

namespace optimizers {

Adam::Adam(const Float _decay_mom1, const Float _decay_mom2,
           const Float _learning_rate, const Float _offset, const size_t _size)
    : decay_mom1_(_decay_mom1),
      decay_mom2_(_decay_mom2),
      first_moment_(std::vector<Float>(_size)),
      learning_rate_(_learning_rate),
      offset_(_offset),
      second_moment_(std::vector<Float>(_size)) {}

}  // namespace optimizers
