// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"

#ifndef ENGINE_COMMANDS_BOOLEANSUBSELECTIONOP_HPP_
#define ENGINE_COMMANDS_BOOLEANSUBSELECTIONOP_HPP_

namespace engine {
namespace commands {

class Column;

/// The command used for boolean subselection operations.
using BooleanSubselectionOp =
    fct::NamedTuple<fct::Field<"operand1_", Column>,
                    fct::Field<"operand2_", Column>,
                    fct::Field<"operator_", fct::Literal<"subselection">>,
                    fct::Field<"type_", fct::Literal<"BooleanColumnView">>>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_BOOLEANSUBSELECTIONOP_HPP_

