// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"

#ifndef ENGINE_COMMANDS_BOOLEANCONSTOP_HPP_
#define ENGINE_COMMANDS_BOOLEANCONSTOP_HPP_

namespace engine {
namespace commands {

/// The possible operators.
using BooleanConstLiteral = fct::Literal<"const">;

/// The command used for boolean binary operations.
using BooleanConstOp =
    fct::NamedTuple<fct::Field<"value_", bool>,
                    fct::Field<"operator_", BooleanConstLiteral>,
                    fct::Field<"type_", fct::Literal<"BooleanColumnView">>>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_BOOLEANCONSTOP_HPP_
