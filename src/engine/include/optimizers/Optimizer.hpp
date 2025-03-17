// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef OPTIMIZERS_OPTIMIZER_HPP_
#define OPTIMIZERS_OPTIMIZER_HPP_

#include "optimizers/Float.hpp"

#include <vector>

namespace optimizers {
// ----------------------------------------------------------------------------

class Optimizer {
 protected:
  virtual ~Optimizer() = default;

 private:
  /// This member functions implements the updating strategy of the optimizer.
  virtual void update_weights(const Float _epoch_num,
                              const std::vector<Float>& _gradients,
                              std::vector<Float>* _weights) = 0;
};

// ----------------------------------------------------------------------------
}  // namespace optimizers

#endif  // OPTIMIZERS_OPTIMIZER_HPP_
