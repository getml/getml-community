// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_STRINGWITHSUBROLESOP_HPP_
#define ENGINE_COMMANDS_STRINGWITHSUBROLESOP_HPP_

#include <string>
#include <vector>

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"

namespace engine {
namespace commands {

class StringColumnOrStringColumnView;

/// The command used for string with subtoles operations.
using StringWithSubrolesOp =
    fct::NamedTuple<fct::Field<"operand1_", StringColumnOrStringColumnView>,
                    fct::Field<"subroles_", std::vector<std::string>>,
                    fct::Field<"type_", fct::Literal<"StringColumnView">>>;

}  // namespace commands
}  // namespace engine

#endif
