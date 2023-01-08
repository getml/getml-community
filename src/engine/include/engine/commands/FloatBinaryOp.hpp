// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_FLOATBINARYOP_HPP_
#define ENGINE_COMMANDS_FLOATBINARYOP_HPP_

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"

namespace engine {
namespace commands {

class FloatColumnOrFloatColumnView;

/// The possible operators.
using FloatBinaryOpLiteral =
    fct::Literal<"divides", "fmod", "minus", "multiplies", "plus", "pow",
                 "update">;

/// The command used for boolean binary operations.
using FloatBinaryOp =
    fct::NamedTuple<fct::Field<"operand1_", FloatColumnOrFloatColumnView>,
                    fct::Field<"operand2_", FloatColumnOrFloatColumnView>,
                    fct::Field<"operator_", FloatBinaryOpLiteral>,
                    fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_FLOATBINARYOP_HPP_
