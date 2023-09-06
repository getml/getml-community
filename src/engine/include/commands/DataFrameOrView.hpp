// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATA_FRAME_OR_VIEW_HPP_
#define COMMANDS_DATA_FRAME_OR_VIEW_HPP_

#include <optional>
#include <string>
#include <vector>

#include "commands/BooleanColumnView.hpp"
#include "commands/FloatColumnOrFloatColumnView.hpp"
#include "commands/StringColumnOrStringColumnView.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"
#include "rfl/TaggedUnion.hpp"

namespace commands {

class DataFrameOrView {
 public:
  /// Operation to add a new column.
  using AddedOp = rfl::NamedTuple<
      rfl::Field<"col_", std::variant<FloatColumnOrFloatColumnView,
                                      StringColumnOrStringColumnView>>,
      rfl::Field<"name_", std::string>, rfl::Field<"role_", std::string>,
      rfl::Field<"subroles_", std::vector<std::string>>,
      rfl::Field<"unit_", std::string>>;

  /// Operation to retrieve a base data frame.
  using DataFrameOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame">>,
                      rfl::Field<"name_", std::string>>;

  /// Operation to parse a view
  using ViewOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"View">>,
      rfl::Field<"name_", std::string>,
      rfl::Field<"base_", rfl::Ref<DataFrameOrView>>,
      rfl::Field<"added_", std::optional<AddedOp>>,
      rfl::Field<"dropped_", std::optional<std::vector<std::string>>>,
      rfl::Field<"last_change_", std::string>,
      rfl::Field<"subselection_",
                 std::optional<std::variant<BooleanColumnView,
                                            FloatColumnOrFloatColumnView>>>>;

  using NamedTupleType = rfl::TaggedUnion<"type_", DataFrameOp, ViewOp>;

  /// Used to break the recursive definition.
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_DATA_FRAME_OR_VIEW_HPP_
