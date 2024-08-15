// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATA_FRAME_OR_VIEW_HPP_
#define COMMANDS_DATA_FRAME_OR_VIEW_HPP_

#include <optional>
#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>
#include <rfl/TaggedUnion.hpp>
#include <string>
#include <vector>

#include "commands/BooleanColumnView.hpp"
#include "commands/FloatColumnOrFloatColumnView.hpp"
#include "commands/StringColumnOrStringColumnView.hpp"

namespace commands {

class DataFrameOrView {
 public:
  /// Operation to add a new column.
  struct AddedOp {
    rfl::Field<"col_", std::variant<FloatColumnOrFloatColumnView,
                                    StringColumnOrStringColumnView>>
        col;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
    rfl::Field<"subroles_", std::vector<std::string>> subroles;
    rfl::Field<"unit_", std::string> unit;
  };

  /// Operation to retrieve a base data frame.
  struct DataFrameOp {
    using Tag = rfl::Literal<"DataFrame">;
    rfl::Field<"name_", std::string> name;
  };

  /// Operation to parse a view
  struct ViewOp {
    using Tag = rfl::Literal<"View">;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"base_", rfl::Ref<DataFrameOrView>> base;
    rfl::Field<"added_", std::optional<AddedOp>> added;
    rfl::Field<"dropped_", std::optional<std::vector<std::string>>> dropped;
    rfl::Field<"last_change_", std::string> last_change;
    rfl::Field<"subselection_",
               std::optional<std::variant<BooleanColumnView,
                                          FloatColumnOrFloatColumnView>>>
        subselection;
  };

  using ReflectionType = rfl::TaggedUnion<"type_", DataFrameOp, ViewOp>;

  /// Used to break the recursive definition.
  ReflectionType val_;
};

}  // namespace commands

#endif  // COMMANDS_DATA_FRAME_OR_VIEW_HPP_
