// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_COLUMN_HPP_
#define ENGINE_COMMANDS_COLUMN_HPP_

#include <string>
#include <variant>

#include "engine/Float.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace commands {

/// Because the different column types depend on each other recursively (as each
/// type can be transformed into the other), we need to have this complex,
/// nested class structure.
class Column {
 public:
};

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_COLUMN_HPP_
