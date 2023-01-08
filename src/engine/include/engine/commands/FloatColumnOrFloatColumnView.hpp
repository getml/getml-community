// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_FLOATCOLUMNORFLOATCOLUMNVIEW_HPP_
#define ENGINE_COMMANDS_FLOATCOLUMNORFLOATCOLUMNVIEW_HPP_

#include <variant>

#include "engine/commands/FloatColumn.hpp"
#include "engine/commands/FloatColumnView.hpp"

namespace engine {
namespace commands {

class FloatColumnOrFloatColumnView {
 public:
  using RecursiveType = std::variant<FloatColumn, FloatColumnView>;

  /// Used to break the recursive definition.
  RecursiveType val_;
};

}  // namespace commands
}  // namespace engine

#endif  //
