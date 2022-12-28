// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"

#ifndef ENGINE_COMMANDS_STRINGCONSTOP_HPP_
#define ENGINE_COMMANDS_STRINGCONSTOP_HPP_

namespace engine {
namespace commands {

/// The command used for string const operations.
using StringConstOp =
    fct::NamedTuple<fct::Field<"value_", std::string>,
                    fct::Field<"operator_", fct::Literal<"const">>,
                    fct::Field<"type_", fct::Literal<"StringColumnView">>>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_STRINGCONSTOP_HPP_
