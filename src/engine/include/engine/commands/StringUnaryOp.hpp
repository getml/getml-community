// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_STRINGUNARYOP_HPP_
#define ENGINE_COMMANDS_STRINGUNARYOP_HPP_

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"

namespace engine {
namespace commands {

class Column;

/// The possible operators.
using StringUnaryOpLiteral =
    fct::Literal<"as_str", "categorical_value", "substr">;

/// The command used for boolean binary operations.
using StringUnaryOp =
    fct::NamedTuple<fct::Field<"operand1_", Column>,
                    fct::Field<"operator_", StringUnaryOpLiteral>,
                    fct::Field<"type_", fct::Literal<"BooleanColumnView">>>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_STRINGUNARYOP_HPP_
