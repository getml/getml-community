// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_COLUMNCOMMAND_HPP_
#define COMMANDS_COLUMNCOMMAND_HPP_

#include <string>
#include <variant>

#include "commands/Aggregation.hpp"
#include "commands/BooleanColumnView.hpp"
#include "commands/FloatColumnOrFloatColumnView.hpp"
#include "commands/Int.hpp"
#include "commands/Pipeline.hpp"
#include "commands/ProjectCommand.hpp"
#include "commands/StringColumnOrStringColumnView.hpp"
#include "json/json.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"
#include "rfl/TaggedUnion.hpp"

namespace commands {

/// Any command to be handled by the ColumnManager.
struct ColumnCommand {
  /// Used as a helper for various commands related to views.
  using ViewCol =
      std::variant<typename FloatColumnOrFloatColumnView::NamedTupleType,
                   typename StringColumnOrStringColumnView::NamedTupleType>;

  /// The command used to aggregate a column
  using AggregationOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"FloatColumn.aggregate",
                                       "StringColumn.aggregate">>,
      rfl::Field<"aggregation_", Aggregation>>;

  /// The command used to add a float column sent by the client
  using FloatColumnOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"FloatColumn">>,
                      rfl::Field<"df_name_", std::string>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"role_", std::string>>;

  /// The command used to retrieve a boolean column.
  using GetBooleanColumnOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"BooleanColumn.get">>,
                      rfl::Field<"col_", BooleanColumnView>>;

  /// The command used to retrieve the content of a boolean column.
  using GetBooleanColumnContentOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"BooleanColumn.get_content">>,
      rfl::Field<"draw_", Int>, rfl::Field<"length_", Int>,
      rfl::Field<"start_", Int>, rfl::Field<"col_", BooleanColumnView>>;

  /// The command used to retrieve the number of rows of a boolean column.
  using GetBooleanColumnNRowsOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"BooleanColumn.nrows",
                                       "BooleanColumn.get_nrows">>,
      rfl::Field<"col_", BooleanColumnView>>;

  /// The command used to get a float column
  using GetFloatColumnOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"FloatColumn.get">>,
                      rfl::Field<"col_", FloatColumnOrFloatColumnView>>;

  /// The command used to retrieve the content of a float column.
  using GetFloatColumnContentOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"FloatColumn.get_content">>,
      rfl::Field<"draw_", Int>, rfl::Field<"length_", Int>,
      rfl::Field<"start_", Int>,
      rfl::Field<"col_", FloatColumnOrFloatColumnView>>;

  /// The command used to retrieve the number of rows of a float column.
  using GetFloatColumnNRowsOp = rfl::NamedTuple<
      rfl::Field<"type_",
                 rfl::Literal<"FloatColumn.nrows", "FloatColumn.get_nrows">>,
      rfl::Field<"col_", FloatColumnOrFloatColumnView>>;

  /// The command used to retrieve the subroles of a float column.
  using GetFloatColumnSubrolesOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"FloatColumn.get_subroles">>,
      rfl::Field<"col_", FloatColumnOrFloatColumnView>>;

  /// The command used to retrieve the unique entries of a float column.
  using GetFloatColumnUniqueOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"FloatColumn.unique">>,
                      rfl::Field<"col_", FloatColumnOrFloatColumnView>>;

  /// The command used to retrieve the unit of a float column.
  using GetFloatColumnUnitOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"FloatColumn.get_unit">>,
                      rfl::Field<"col_", FloatColumnOrFloatColumnView>>;

  /// The command used to get a string column
  using GetStringColumnOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"StringColumn.get">>,
                      rfl::Field<"col_", StringColumnOrStringColumnView>>;

  /// The command used to retrieve the content of a float column.
  using GetStringColumnContentOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"StringColumn.get_content">>,
      rfl::Field<"draw_", Int>, rfl::Field<"length_", Int>,
      rfl::Field<"start_", Int>,
      rfl::Field<"col_", StringColumnOrStringColumnView>>;

  /// The command used to retrieve the number of rows of a float column.
  using GetStringColumnNRowsOp = rfl::NamedTuple<
      rfl::Field<"type_",
                 rfl::Literal<"StringColumn.nrows", "StringColumn.get_nrows">>,
      rfl::Field<"col_", StringColumnOrStringColumnView>>;

  /// The command used to retrieve the subroles of a float column.
  using GetStringColumnSubrolesOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"StringColumn.get_subroles">>,
      rfl::Field<"col_", StringColumnOrStringColumnView>>;

  /// The command used to retrieve the unique entries of a float column.
  using GetStringColumnUniqueOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"StringColumn.unique">>,
                      rfl::Field<"col_", StringColumnOrStringColumnView>>;

  /// The command used to retrieve the unit of a float column.
  using GetStringColumnUnitOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"StringColumn.get_unit">>,
      rfl::Field<"col_", StringColumnOrStringColumnView>>;

  /// The command used to set the subroles of a float column.
  using SetFloatColumnSubrolesOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"FloatColumn.set_subroles">>,
      rfl::Field<"df_name_", std::string>, rfl::Field<"name_", std::string>,
      rfl::Field<"role_", std::string>,
      rfl::Field<"subroles_", std::vector<std::string>>>;

  /// The command used to set the unit of a float column.
  using SetFloatColumnUnitOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"FloatColumn.set_unit">>,
      rfl::Field<"df_name_", std::string>, rfl::Field<"name_", std::string>,
      rfl::Field<"role_", std::string>, rfl::Field<"unit_", std::string>>;

  /// The command used to set the subroles of a string column.
  using SetStringColumnSubrolesOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"StringColumn.set_subroles">>,
      rfl::Field<"df_name_", std::string>, rfl::Field<"name_", std::string>,
      rfl::Field<"role_", std::string>,
      rfl::Field<"subroles_", std::vector<std::string>>>;

  /// The command used to set the unit of a string column.
  using SetStringColumnUnitOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"StringColumn.set_unit">>,
      rfl::Field<"df_name_", std::string>, rfl::Field<"name_", std::string>,
      rfl::Field<"role_", std::string>, rfl::Field<"unit_", std::string>>;

  /// The command used to add a string column sent by the client
  using StringColumnOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"StringColumn">>,
                      rfl::Field<"df_name_", std::string>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"role_", std::string>>;

  using NamedTupleType = rfl::TaggedUnion<
      "type_", AggregationOp, FloatColumnOp, GetBooleanColumnOp,
      GetBooleanColumnContentOp, GetBooleanColumnNRowsOp, GetFloatColumnOp,
      GetFloatColumnContentOp, GetFloatColumnNRowsOp, GetFloatColumnSubrolesOp,
      GetFloatColumnUniqueOp, GetFloatColumnUnitOp, GetStringColumnOp,
      GetStringColumnContentOp, GetStringColumnNRowsOp,
      GetStringColumnSubrolesOp, GetStringColumnUniqueOp, GetStringColumnUnitOp,
      SetFloatColumnSubrolesOp, SetFloatColumnUnitOp, SetStringColumnSubrolesOp,
      SetStringColumnUnitOp, StringColumnOp>;

  using InputVarType = typename json::Reader::InputVarType;

  static ColumnCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_COLUMNCOMMAND_HPP_
