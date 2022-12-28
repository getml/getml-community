// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"

#ifndef ENGINE_COMMANDS_BOOLEANBINARYOP_HPP_
#define ENGINE_COMMANDS_BOOLEANBINARYOP_HPP_

namespace engine {
namespace commands {

class Column;

/// The possible operators.
using BooleanBinaryOpLiteral =
    fct::Literal<"and", "contains", "equal_to", "greater", "greater_equal",
                 "less", "less_equal", "not_equal_to", "or", "xor">;

/// The command used for boolean binary operations.
using BooleanBinaryOp =
    fct::NamedTuple<fct::Field<"operand1_", Column>,
                    fct::Field<"operand2_", Column>,
                    fct::Field<"operator_", BooleanBinaryOpLiteral>,
                    fct::Field<"type_", fct::Literal<"BooleanColumnView">>>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_BOOLEANBINARYOP_HPP_
