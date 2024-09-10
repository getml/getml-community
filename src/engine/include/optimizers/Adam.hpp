// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef OPTIMIZERS_ADAM_HPP_
#define OPTIMIZERS_ADAM_HPP_

// ----------------------------------------------------------------------------

#include <cmath>
#include <vector>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"

// ----------------------------------------------------------------------------

#include "optimizers/Float.hpp"
#include "optimizers/Int.hpp"
#include "optimizers/Optimizer.hpp"

// ----------------------------------------------------------------------------

namespace optimizers {

// ----------------------------------------------------------------------------

class Adam : public Optimizer {
 public:
  Adam(const Float _decay_mom1, const Float _decay_mom2,
       const Float _learning_rate, const Float _offset, const size_t _size)
      : decay_mom1_(_decay_mom1),
        decay_mom2_(_decay_mom2),
        first_moment_(std::vector<Float>(_size)),
        learning_rate_(_learning_rate),
        offset_(_offset),
        second_moment_(std::vector<Float>(_size)) {}

  ~Adam() = default;

  // ----------------------------------------------------------------------------

  /// This member functions implements the updating strategy of the optimizer.
  void update_weights(const Float _epoch_num,
                      const std::vector<Float>& _gradients,
                      std::vector<Float>* _weights) final {
    assert_true(_gradients.size() == _weights->size());
    assert_true(_gradients.size() == first_moment_.size());
    assert_true(_gradients.size() == second_moment_.size());

    for (size_t i = 0; i < _weights->size(); ++i) {
      update(_gradients[i], _epoch_num, &first_moment_[i], &second_moment_[i],
             &(*_weights)[i]);
    }
  }

  // ----------------------------------------------------------------------------

 private:
  // Does the actual Adam updates.
  void update(const Float _g, const Float _epoch_num, Float* _mom1,
              Float* _mom2, Float* _w) const {
    *_mom1 = *_mom1 * decay_mom1_ + (1.0 - decay_mom1_) * _g;

    *_mom2 = *_mom2 * decay_mom2_ + (1.0 - decay_mom2_) * _g * _g;

    const auto numerator =
        *_mom1 / (1.0 - std::pow(decay_mom1_, _epoch_num + 1.0));

    const auto divisor =
        std::sqrt(*_mom2 / (1.0 - std::pow(decay_mom2_, _epoch_num + 1.0))) +
        offset_;

    *_w -= learning_rate_ * numerator / divisor;
  }

  // ----------------------------------------------------------------------------

 private:
  /// Decay rate for the first moment.
  const Float decay_mom1_;

  /// Decay rate for the second moment.
  const Float decay_mom2_;

  // Keeping "intertia" is a core idea of the Adam algorithm.
  std::vector<Float> first_moment_;

  /// The learning rate used for the Adam algorithm.
  const Float learning_rate_;

  /// The offset prevents us from dividing by zero.
  const Float offset_;

  // Dividing by the sum of squared gradients is one core idea of the Adam
  // algorithm.
  std::vector<Float> second_moment_;
};

// ----------------------------------------------------------------------------
}  // namespace optimizers

#endif  // OPTIMIZERS_ADAM_HPP_
