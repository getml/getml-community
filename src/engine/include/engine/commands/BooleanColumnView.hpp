// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include <variant>

#include "engine/commands/BooleanBinaryOp.hpp"
#include "engine/commands/BooleanConstOp.hpp"
#include "engine/commands/BooleanSubselectionOp.hpp"
#include "engine/commands/BooleanUnaryOp.hpp"

#ifndef ENGINE_COMMANDS_BOOLEANBINARYOP_HPP_
#define ENGINE_COMMANDS_BOOLEANBINARYOP_HPP_

namespace engine {
namespace commands {

/// Defines a boolean column view.
using BooleanColumnView = std::variant<BooleanBinaryOp, BooleanConstOp,
                                       BooleanSubselectionOp, BooleanUnaryOp>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_BOOLEANBINARYOP_HPP_
