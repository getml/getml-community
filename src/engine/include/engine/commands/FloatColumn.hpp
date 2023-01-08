// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_FLOATCOLUMN_HPP_
#define ENGINE_COMMANDS_FLOATCOLUMN_HPP_

#include <string>

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"

namespace engine {
namespace commands {

/// The command used for retrieving float columns from a data frame.
using FloatColumn =
    fct::NamedTuple<fct::Field<"df_name_", std::string>,
                    fct::Field<"name_", std::string>,
                    fct::Field<"type_", fct::Literal<"FloatColumn">>>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_FLOATCOLUMN_HPP_
