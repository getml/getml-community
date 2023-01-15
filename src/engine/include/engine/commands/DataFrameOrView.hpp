// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_DATA_FRAME_OR_VIEW_HPP_
#define ENGINE_COMMANDS_DATA_FRAME_OR_VIEW_HPP_

#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "engine/commands/BooleanColumnView.hpp"
#include "engine/commands/FloatColumnOrFloatColumnView.hpp"
#include "engine/commands/StringColumnOrStringColumnView.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace commands {

class DataFrameOrView {
 public:
  /// Operation to add a new column.
  using AddedOp = fct::NamedTuple<
      fct::Field<"col_", std::variant<FloatColumnOrFloatColumnView,
                                      StringColumnOrStringColumnView>>,
      fct::Field<"name_", std::string>, fct::Field<"role_", std::string>,
      fct::Field<"subroles_", std::vector<std::string>>,
      fct::Field<"unit_", std::string>>;

  /// Operation to retrieve a base data frame.
  using DataFrameOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame">>,
                      fct::Field<"name_", std::string>>;

  /// Operation to parse a view
  using ViewOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"View">>,
      fct::Field<"base_", fct::Ref<DataFrameOrView>>,
      fct::Field<"added_", std::optional<AddedOp>>,
      fct::Field<"dropped_", std::optional<std::vector<std::string>>>,
      fct::Field<"subselection_",
                 std::optional<std::variant<BooleanColumnView,
                                            FloatColumnOrFloatColumnView>>>>;

  using RecursiveType = std::variant<DataFrameOp, ViewOp>;

  /// Used to break the recursive definition.
  RecursiveType val_;
};

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_DATA_FRAME_OR_VIEW_HPP_
