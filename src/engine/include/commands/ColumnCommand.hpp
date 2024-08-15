// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_COLUMNCOMMAND_HPP_
#define COMMANDS_COLUMNCOMMAND_HPP_

#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>
#include <rfl/TaggedUnion.hpp>
#include <rfl/json/Reader.hpp>
#include <string>
#include <variant>

#include "commands/Aggregation.hpp"
#include "commands/BooleanColumnView.hpp"
#include "commands/FloatColumnOrFloatColumnView.hpp"
#include "commands/Int.hpp"
#include "commands/StringColumnOrStringColumnView.hpp"

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
    using Tag = rfl::Literal<"FloatColumn">;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
  };

  /// The command used to retrieve a boolean column.
  struct GetBooleanColumnOp {
    using Tag = rfl::Literal<"BooleanColumn.get">;
    rfl::Field<"col_", BooleanColumnView> col;
  };

  /// The command used to retrieve the content of a boolean column.
  struct GetBooleanColumnContentOp {
    using Tag = rfl::Literal<"BooleanColumn.get_content">;
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
    using Tag = rfl::Literal<"FloatColumn.get">;
    rfl::Field<"col_", FloatColumnOrFloatColumnView> col;
  };

  /// The command used to retrieve the content of a float column.
  struct GetFloatColumnContentOp {
    using Tag = rfl::Literal<"FloatColumn.get_content">;
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
    using Tag = rfl::Literal<"FloatColumn.get_subroles">;
    rfl::Field<"col_", FloatColumnOrFloatColumnView> col;
  };

  /// The command used to retrieve the unique entries of a float column.
  struct GetFloatColumnUniqueOp {
    using Tag = rfl::Literal<"FloatColumn.unique">;
    rfl::Field<"col_", FloatColumnOrFloatColumnView> col;
  };

  /// The command used to retrieve the unit of a float column.
  struct GetFloatColumnUnitOp {
    using Tag = rfl::Literal<"FloatColumn.get_unit">;
    rfl::Field<"col_", FloatColumnOrFloatColumnView> col;
  };

  /// The command used to get a string column
  struct GetStringColumnOp {
    using Tag = rfl::Literal<"StringColumn.get">;
    rfl::Field<"col_", StringColumnOrStringColumnView> col;
  };

  /// The command used to retrieve the content of a float column.
  struct GetStringColumnContentOp {
    using Tag = rfl::Literal<"StringColumn.get_content">;
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
    using Tag = rfl::Literal<"StringColumn.get_subroles">;
    rfl::Field<"col_", StringColumnOrStringColumnView> col;
  };

  /// The command used to retrieve the unique entries of a float column.
  struct GetStringColumnUniqueOp {
    using Tag = rfl::Literal<"StringColumn.unique">;
    rfl::Field<"col_", StringColumnOrStringColumnView> col;
  };

  /// The command used to retrieve the unit of a float column.
  struct GetStringColumnUnitOp {
    using Tag = rfl::Literal<"StringColumn.get_unit">;
    rfl::Field<"col_", StringColumnOrStringColumnView> col;
  };

  /// The command used to set the subroles of a float column.
  struct SetFloatColumnSubrolesOp {
    using Tag = rfl::Literal<"FloatColumn.set_subroles">;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
    rfl::Field<"subroles_", std::vector<std::string>> subroles;
  };

  /// The command used to set the unit of a float column.
  struct SetFloatColumnUnitOp {
    using Tag = rfl::Literal<"FloatColumn.set_unit">;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
    rfl::Field<"unit_", std::string> unit;
  };

  /// The command used to set the subroles of a string column.
  struct SetStringColumnSubrolesOp {
    using Tag = rfl::Literal<"StringColumn.set_subroles">;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
    rfl::Field<"subroles_", std::vector<std::string>> subroles;
  };

  /// The command used to set the unit of a string column.
  struct SetStringColumnUnitOp {
    using Tag = rfl::Literal<"StringColumn.set_unit">;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
    rfl::Field<"unit_", std::string> unit;
  };

  /// The command used to add a string column sent by the client
  struct StringColumnOp {
    using Tag = rfl::Literal<"StringColumn">;
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

  using InputVarType = typename rfl::json::Reader::InputVarType;

  static ColumnCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  ReflectionType val_;
};

}  // namespace commands

#endif  // COMMANDS_COLUMNCOMMAND_HPP_
