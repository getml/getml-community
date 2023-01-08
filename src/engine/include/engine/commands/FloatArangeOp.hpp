// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_FLOATARANGEOP_HPP_
#define ENGINE_COMMANDS_FLOATARANGEOP_HPP_

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"

namespace engine {
namespace commands {

/// The command used for arange operations.
using FloatArangeOp =
    fct::NamedTuple<fct::Field<"start_", Float>, fct::Field<"stop_", Float>,
                    fct::Field<"step_", Float>,
                    fct::Field<"operator_", fct::Literal<"arange">>,
                    fct::Field<"type_", fct::Literal<"FloatColumnView">>>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_FLOATARANGEOP_HPP_
