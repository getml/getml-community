// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_FLOATSUBSELECTIONOP_HPP_
#define ENGINE_COMMANDS_FLOATSUBSELECTIONOP_HPP_

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"

namespace engine {
namespace commands {

class Column;

/// The command used for float subselection operations.
using FloatSubselectionOp =
    fct::NamedTuple<fct::Field<"operand1_", Column>,
                    fct::Field<"operand2_", Column>,
                    fct::Field<"operator_", fct::Literal<"subselection">>,
                    fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_FLOATSUBSELECTIONOP_HPP_

