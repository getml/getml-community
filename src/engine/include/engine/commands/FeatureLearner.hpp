// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_FEATURELEARNER_HPP_
#define ENGINE_COMMANDS_FEATURELEARNER_HPP_

#include <variant>

#include "fastprop/Hyperparameters.hpp"

namespace engine {
namespace commands {

class FeatureLearner {
 public:
  using NamedTupleType = std::variant<fastprop::Hyperparameters>;

  NamedTupleType val_;
};

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_FEATURELEARNER_HPP_
