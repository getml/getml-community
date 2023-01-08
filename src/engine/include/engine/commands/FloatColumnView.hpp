// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_FLOATCOLUMNVIEW_HPP_
#define ENGINE_COMMANDS_FLOATCOLUMNVIEW_HPP_

#include <variant>

#include "engine/commands/FloatArangeOp.hpp"
#include "engine/commands/FloatBinaryOp.hpp"
#include "engine/commands/FloatConstOp.hpp"
#include "engine/commands/FloatSubselectionOp.hpp"
#include "engine/commands/FloatUnaryOp.hpp"
#include "engine/commands/FloatWithSubrolesOp.hpp"
#include "engine/commands/FloatWithUnitOp.hpp"

namespace engine {
namespace commands {

/// Defines a float column view.
using FloatColumnView = std::variant<FloatArangeOp, FloatBinaryOp, FloatConstOp,
                                     FloatSubselectionOp, FloatUnaryOp,
                                     FloatWithSubrolesOp, FloatWithUnitOp>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_FLOATCOLUMNVIEW_HPP_
