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
      std::variant<typename FloatColumnOrFloatColumnView::ReflectionType,
                   typename StringColumnOrStringColumnView::ReflectionType>;

  /// The command used to aggregate a column
  struct AggregationOp {
    rfl::Field<"type_",
               rfl::Literal<"FloatColumn.aggregate", "StringColumn.aggregate">>
        type;
    rfl::Field<"aggregation_", Aggregation> aggregation;
  };

  /// The command used to add a float column sent by the client
  struct FloatColumnOp {
    rfl::Field<"type_", rfl::Literal<"FloatColumn">> type;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
  };

  /// The command used to retrieve a boolean column.
  struct GetBooleanColumnOp {
    rfl::Field<"type_", rfl::Literal<"BooleanColumn.get">> type;
    rfl::Field<"col_", BooleanColumnView> col;
  };

  /// The command used to retrieve the content of a boolean column.
  struct GetBooleanColumnContentOp {
    rfl::Field<"type_", rfl::Literal<"BooleanColumn.get_content">> type;
    rfl::Field<"draw_", Int> draw;
    rfl::Field<"length_", Int> length;
    rfl::Field<"start_", Int> start;
    rfl::Field<"col_", BooleanColumnView> col;
  };

  /// The command used to retrieve the number of rows of a boolean column.
  struct GetBooleanColumnNRowsOp {
    rfl::Field<"type_",
               rfl::Literal<"BooleanColumn.nrows", "BooleanColumn.get_nrows">>
        type;
    rfl::Field<"col_", BooleanColumnView> col;
  };

  /// The command used to get a float column
  struct GetFloatColumnOp {
    rfl::Field<"type_", rfl::Literal<"FloatColumn.get">> type;
    rfl::Field<"col_", FloatColumnOrFloatColumnView> col;
  };

  /// The command used to retrieve the content of a float column.
  struct GetFloatColumnContentOp {
    rfl::Field<"type_", rfl::Literal<"FloatColumn.get_content">> type;
    rfl::Field<"draw_", Int> draw;
    rfl::Field<"length_", Int> length;
    rfl::Field<"start_", Int> start;
    rfl::Field<"col_", FloatColumnOrFloatColumnView> col;
  };

  /// The command used to retrieve the number of rows of a float column.
  struct GetFloatColumnNRowsOp {
    rfl::Field<"type_",
               rfl::Literal<"FloatColumn.nrows", "FloatColumn.get_nrows">>
        type;
    rfl::Field<"col_", FloatColumnOrFloatColumnView> col;
  };

  /// The command used to retrieve the subroles of a float column.
  struct GetFloatColumnSubrolesOp {
    rfl::Field<"type_", rfl::Literal<"FloatColumn.get_subroles">> type;
    rfl::Field<"col_", FloatColumnOrFloatColumnView> col;
  };

  /// The command used to retrieve the unique entries of a float column.
  struct GetFloatColumnUniqueOp {
    rfl::Field<"type_", rfl::Literal<"FloatColumn.unique">> type;
    rfl::Field<"col_", FloatColumnOrFloatColumnView> col;
  };

  /// The command used to retrieve the unit of a float column.
  struct GetFloatColumnUnitOp {
    rfl::Field<"type_", rfl::Literal<"FloatColumn.get_unit">> type;
    rfl::Field<"col_", FloatColumnOrFloatColumnView> col;
  };

  /// The command used to get a string column
  struct GetStringColumnOp {
    rfl::Field<"type_", rfl::Literal<"StringColumn.get">> type;
    rfl::Field<"col_", StringColumnOrStringColumnView> col;
  };

  /// The command used to retrieve the content of a float column.
  struct GetStringColumnContentOp {
    rfl::Field<"type_", rfl::Literal<"StringColumn.get_content">> type;
    rfl::Field<"draw_", Int> draw;
    rfl::Field<"length_", Int> length;
    rfl::Field<"start_", Int> start;
    rfl::Field<"col_", StringColumnOrStringColumnView> col;
  };

  /// The command used to retrieve the number of rows of a float column.
  struct GetStringColumnNRowsOp {
    rfl::Field<"type_",
               rfl::Literal<"StringColumn.nrows", "StringColumn.get_nrows">>
        type;
    rfl::Field<"col_", StringColumnOrStringColumnView> col;
  };

  /// The command used to retrieve the subroles of a float column.
  struct GetStringColumnSubrolesOp {
    rfl::Field<"type_", rfl::Literal<"StringColumn.get_subroles">> type;
    rfl::Field<"col_", StringColumnOrStringColumnView> col;
  };

  /// The command used to retrieve the unique entries of a float column.
  struct GetStringColumnUniqueOp {
    rfl::Field<"type_", rfl::Literal<"StringColumn.unique">> type;
    rfl::Field<"col_", StringColumnOrStringColumnView> col;
  };

  /// The command used to retrieve the unit of a float column.
  struct GetStringColumnUnitOp {
    rfl::Field<"type_", rfl::Literal<"StringColumn.get_unit">> type;
    rfl::Field<"col_", StringColumnOrStringColumnView> col;
  };

  /// The command used to set the subroles of a float column.
  struct SetFloatColumnSubrolesOp {
    rfl::Field<"type_", rfl::Literal<"FloatColumn.set_subroles">> type;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
    rfl::Field<"subroles_", std::vector<std::string>> subroles;
  };

  /// The command used to set the unit of a float column.
  struct SetFloatColumnUnitOp {
    rfl::Field<"type_", rfl::Literal<"FloatColumn.set_unit">> type;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
    rfl::Field<"unit_", std::string> unit;
  };

  /// The command used to set the subroles of a string column.
  struct SetStringColumnSubrolesOp {
    rfl::Field<"type_", rfl::Literal<"StringColumn.set_subroles">> type;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
    rfl::Field<"subroles_", std::vector<std::string>> subroles;
  };

  /// The command used to set the unit of a string column.
  struct SetStringColumnUnitOp {
    rfl::Field<"type_", rfl::Literal<"StringColumn.set_unit">> type;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
    rfl::Field<"unit_", std::string> unit;
  };

  /// The command used to add a string column sent by the client
  struct StringColumnOp {
    rfl::Field<"type_", rfl::Literal<"StringColumn">> type;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
  };

  using ReflectionType = rfl::TaggedUnion<
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
  ReflectionType val_;
};

}  // namespace commands

#endif  // COMMANDS_COLUMNCOMMAND_HPP_
