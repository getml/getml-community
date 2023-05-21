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
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "fct/TaggedUnion.hpp"
#include "json/json.hpp"

namespace commands {

/// Any command to be handled by the ColumnManager.
struct ColumnCommand {
  /// Used as a helper for various commands related to views.
  using ViewCol =
      std::variant<typename FloatColumnOrFloatColumnView::NamedTupleType,
                   typename StringColumnOrStringColumnView::NamedTupleType>;

  /// The command used to aggregate a column
  using AggregationOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"FloatColumn.aggregate",
                                       "StringColumn.aggregate">>,
      fct::Field<"aggregation_", Aggregation>>;

  /// The command used to add a float column sent by the client
  using FloatColumnOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"FloatColumn">>,
                      fct::Field<"df_name_", std::string>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"role_", std::string>>;

  /// The command used to retrieve a boolean column.
  using GetBooleanColumnOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"BooleanColumn.get">>,
                      fct::Field<"col_", BooleanColumnView>>;

  /// The command used to retrieve the content of a boolean column.
  using GetBooleanColumnContentOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"BooleanColumn.get_content">>,
      fct::Field<"draw_", Int>, fct::Field<"length_", Int>,
      fct::Field<"start_", Int>, fct::Field<"col_", BooleanColumnView>>;

  /// The command used to retrieve the number of rows of a boolean column.
  using GetBooleanColumnNRowsOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"BooleanColumn.nrows",
                                       "BooleanColumn.get_nrows">>,
      fct::Field<"col_", BooleanColumnView>>;

  /// The command used to get a float column
  using GetFloatColumnOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"FloatColumn.get">>,
                      fct::Field<"col_", FloatColumnOrFloatColumnView>>;

  /// The command used to retrieve the content of a float column.
  using GetFloatColumnContentOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"FloatColumn.get_content">>,
      fct::Field<"draw_", Int>, fct::Field<"length_", Int>,
      fct::Field<"start_", Int>,
      fct::Field<"col_", FloatColumnOrFloatColumnView>>;

  /// The command used to retrieve the number of rows of a float column.
  using GetFloatColumnNRowsOp = fct::NamedTuple<
      fct::Field<"type_",
                 fct::Literal<"FloatColumn.nrows", "FloatColumn.get_nrows">>,
      fct::Field<"col_", FloatColumnOrFloatColumnView>>;

  /// The command used to retrieve the subroles of a float column.
  using GetFloatColumnSubrolesOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"FloatColumn.get_subroles">>,
      fct::Field<"col_", FloatColumnOrFloatColumnView>>;

  /// The command used to retrieve the unique entries of a float column.
  using GetFloatColumnUniqueOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"FloatColumn.unique">>,
                      fct::Field<"col_", FloatColumnOrFloatColumnView>>;

  /// The command used to retrieve the unit of a float column.
  using GetFloatColumnUnitOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"FloatColumn.get_unit">>,
                      fct::Field<"col_", FloatColumnOrFloatColumnView>>;

  /// The command used to get a string column
  using GetStringColumnOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"StringColumn.get">>,
                      fct::Field<"col_", StringColumnOrStringColumnView>>;

  /// The command used to retrieve the content of a float column.
  using GetStringColumnContentOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"StringColumn.get_content">>,
      fct::Field<"draw_", Int>, fct::Field<"length_", Int>,
      fct::Field<"start_", Int>,
      fct::Field<"col_", StringColumnOrStringColumnView>>;

  /// The command used to retrieve the number of rows of a float column.
  using GetStringColumnNRowsOp = fct::NamedTuple<
      fct::Field<"type_",
                 fct::Literal<"StringColumn.nrows", "StringColumn.get_nrows">>,
      fct::Field<"col_", StringColumnOrStringColumnView>>;

  /// The command used to retrieve the subroles of a float column.
  using GetStringColumnSubrolesOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"StringColumn.get_subroles">>,
      fct::Field<"col_", StringColumnOrStringColumnView>>;

  /// The command used to retrieve the unique entries of a float column.
  using GetStringColumnUniqueOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"StringColumn.unique">>,
                      fct::Field<"col_", StringColumnOrStringColumnView>>;

  /// The command used to retrieve the unit of a float column.
  using GetStringColumnUnitOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"StringColumn.get_unit">>,
      fct::Field<"col_", StringColumnOrStringColumnView>>;

  /// The command used to set the subroles of a float column.
  using SetFloatColumnSubrolesOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"FloatColumn.set_subroles">>,
      fct::Field<"df_name_", std::string>, fct::Field<"name_", std::string>,
      fct::Field<"role_", std::string>,
      fct::Field<"subroles_", std::vector<std::string>>>;

  /// The command used to set the unit of a float column.
  using SetFloatColumnUnitOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"FloatColumn.set_unit">>,
      fct::Field<"df_name_", std::string>, fct::Field<"name_", std::string>,
      fct::Field<"role_", std::string>, fct::Field<"unit_", std::string>>;

  /// The command used to set the subroles of a string column.
  using SetStringColumnSubrolesOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"StringColumn.set_subroles">>,
      fct::Field<"df_name_", std::string>, fct::Field<"name_", std::string>,
      fct::Field<"role_", std::string>,
      fct::Field<"subroles_", std::vector<std::string>>>;

  /// The command used to set the unit of a string column.
  using SetStringColumnUnitOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"StringColumn.set_unit">>,
      fct::Field<"df_name_", std::string>, fct::Field<"name_", std::string>,
      fct::Field<"role_", std::string>, fct::Field<"unit_", std::string>>;

  /// The command used to add a string column sent by the client
  using StringColumnOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"StringColumn">>,
                      fct::Field<"df_name_", std::string>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"role_", std::string>>;

  using NamedTupleType = fct::TaggedUnion<
      "type_", AggregationOp, FloatColumnOp, GetBooleanColumnOp,
      GetBooleanColumnContentOp, GetBooleanColumnNRowsOp, GetFloatColumnOp,
      GetFloatColumnContentOp, GetFloatColumnNRowsOp, GetFloatColumnSubrolesOp,
      GetFloatColumnUniqueOp, GetFloatColumnUnitOp, GetStringColumnOp,
      GetStringColumnContentOp, GetStringColumnNRowsOp,
      GetStringColumnSubrolesOp, GetStringColumnUniqueOp, GetStringColumnUnitOp,
      SetFloatColumnSubrolesOp, SetFloatColumnUnitOp, SetStringColumnSubrolesOp,
      SetStringColumnUnitOp, StringColumnOp>;

  using InputVarType = typename json::JSONParser::InputVarType;

  static ColumnCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_COLUMNCOMMAND_HPP_
