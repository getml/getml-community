// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include <string>

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"

#ifndef ENGINE_COMMANDS_STRINGWITHUNITOP_HPP_
#define ENGINE_COMMANDS_STRINGWITHUNITOP_HPP_

namespace engine {
namespace commands {

class StringColumnOrStringColumnView;

/// The command used for string with unit operations.
using StringWithUnitOp =
    fct::NamedTuple<fct::Field<"operand1_", StringColumnOrStringColumnView>,
                    fct::Field<"unit_", std::string>,
                    fct::Field<"type_", fct::Literal<"StringColumnView">>>;

}  // namespace commands
}  // namespace engine

#endif
