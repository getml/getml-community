// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_VIEWCOMMAND_HPP_
#define COMMANDS_VIEWCOMMAND_HPP_

#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>
#include <rfl/TaggedUnion.hpp>
#include <rfl/json/Reader.hpp>
#include <string>
#include <variant>

#include "commands/DataFrameOrView.hpp"
#include "commands/FloatColumnOrFloatColumnView.hpp"
#include "commands/Int.hpp"
#include "commands/StringColumnOrStringColumnView.hpp"

namespace commands {

/// Any command to be handled by the ViewManager.
struct ViewCommand {
  /// Used as a helper for various commands related to views.
  using ViewCol =
      std::variant<typename FloatColumnOrFloatColumnView::ReflectionType,
                   typename StringColumnOrStringColumnView::ReflectionType>;

  /// The command used to retrieve the content of a view.
  struct GetViewContentOp {
    using Tag = rfl::Literal<"View.get_content">;
    rfl::Field<"cols_", std::vector<ViewCol>> cols;
    rfl::Field<"draw_", Int> draw;
    rfl::Field<"length_", Int> length;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"start_", Int> start;
  };

  /// The command used to retrieve the number of rows of a view.
  struct GetViewNRowsOp {
    using Tag = rfl::Literal<"View.get_nrows">;
    rfl::Field<"cols_", std::vector<ViewCol>> cols;
    rfl::Field<"force_", bool> force;
  };

  /// The command used to transform a view to Arrow.
  struct ViewToArrowOp {
    using Tag = rfl::Literal<"View.to_arrow">;
    rfl::Field<"view_", DataFrameOrView> view;
  };

  /// The command used to transform a view to CSV.
  struct ViewToCSVOp {
    using Tag = rfl::Literal<"View.to_csv">;
    rfl::Field<"view_", DataFrameOrView> view;
    rfl::Field<"fname_", std::string> fname;
    rfl::Field<"batch_size_", size_t> batch_size;
    rfl::Field<"quotechar_", std::string> quotechar;
    rfl::Field<"sep_", std::string> sep;
  };

  /// The command used to write a view into the database.
  struct ViewToDBOp {
    using Tag = rfl::Literal<"View.to_db">;
    rfl::Field<"view_", DataFrameOrView> view;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"table_name_", std::string> table_name;
  };

  /// The command used to transform a view to parquet.
  struct ViewToParquetOp {
    using Tag = rfl::Literal<"View.to_parquet">;
    rfl::Field<"view_", DataFrameOrView> view;
    rfl::Field<"fname_", std::string> fname;
    rfl::Field<"compression_", std::string> compression;
  };

  using ReflectionType =
      rfl::TaggedUnion<"type_", GetViewContentOp, GetViewNRowsOp, ViewToArrowOp,
                       ViewToCSVOp, ViewToDBOp, ViewToParquetOp>;

  using InputVarType = typename rfl::json::Reader::InputVarType;

  static ViewCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  ReflectionType val_;
};

}  // namespace commands

#endif  // COMMANDS_VIEWCOMMAND_HPP_
