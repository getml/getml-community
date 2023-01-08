// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_FLOATUNARYOP_HPP_
#define ENGINE_COMMANDS_FLOATUNARYOP_HPP_

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"

namespace engine {
namespace commands {

class Column;

/// The possible operators.
using FloatUnaryOpLiteral =
    fct::Literal<"abs", "acos", "as_num", "as_ts", "asin", "atan",
                 "boolean_as_num", "cbrt", "ceil", "cos", "day", "erf", "exp",
                 "floor", "hour", "lgamma", "log", "minute", "month", "random",
                 "round", "rowid", "second", "sin", "sqrt", "tan", "tgamma",
                 "value", "weekday", "year", "yearday">;

/// The command used for float unary operations.
using FloatUnaryOp =
    fct::NamedTuple<fct::Field<"operand1_", Column>,
                    fct::Field<"operator_", FloatUnaryOpLiteral>,
                    fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_FLOATUNARYOP_HPP_
