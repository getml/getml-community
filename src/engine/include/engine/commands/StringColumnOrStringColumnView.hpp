// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include <variant>

#include "engine/commands/StringColumn.hpp"
#include "engine/commands/StringColumnView.hpp"

#ifndef ENGINE_COMMANDS_STRINGCOLUMNORSTRINGCOLUMNVIEW_HPP_
#define ENGINE_COMMANDS_STRINGCOLUMNORSTRINGCOLUMNVIEW_HPP_

namespace engine {
namespace commands {

class StringColumnOrStringColumnView {
 public:
  using RecursiveType = std::variant<StringColumn, StringColumnView>;

  /// Used to break the recursive definition.
  RecursiveType val_;
};

}  // namespace commands
}  // namespace engine

#endif  //
