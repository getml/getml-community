// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include <variant>

#include "engine/commands/StringBinaryOp.hpp"
#include "engine/commands/StringConstOp.hpp"
#include "engine/commands/StringSubselectionOp.hpp"
#include "engine/commands/StringUnaryOp.hpp"
#include "engine/commands/StringWithSubrolesOp.hpp"
#include "engine/commands/StringWithUnitOp.hpp"

#ifndef ENGINE_COMMANDS_STRINGCOLUMNVIEW_HPP_
#define ENGINE_COMMANDS_STRINGCOLUMNVIEW_HPP_

namespace engine {
namespace commands {

/// Defines a string column view.
using StringColumnView =
    std::variant<StringBinaryOp, StringConstOp, StringSubselectionOp,
                 StringUnaryOp, StringWithSubrolesOp, StringWithUnitOp>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_STRINGCOLUMNVIEW_HPP_
