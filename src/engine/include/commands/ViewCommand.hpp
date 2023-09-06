// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_VIEWCOMMAND_HPP_
#define COMMANDS_VIEWCOMMAND_HPP_

#include <string>
#include <variant>

#include "commands/DataFrameOrView.hpp"
#include "commands/FloatColumnOrFloatColumnView.hpp"
#include "commands/Int.hpp"
#include "commands/StringColumnOrStringColumnView.hpp"
#include "json/json.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"
#include "rfl/TaggedUnion.hpp"

namespace commands {

/// Any command to be handled by the ViewManager.
struct ViewCommand {
  /// Used as a helper for various commands related to views.
  using ViewCol =
      std::variant<typename FloatColumnOrFloatColumnView::NamedTupleType,
                   typename StringColumnOrStringColumnView::NamedTupleType>;

  /// The command used to retrieve the content of a view.
  using GetViewContentOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"View.get_content">>,
                      rfl::Field<"cols_", std::vector<ViewCol>>,
                      rfl::Field<"draw_", Int>, rfl::Field<"length_", Int>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"start_", Int>>;

  /// The command used to retrieve the number of rows of a view.
  using GetViewNRowsOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"View.get_nrows">>,
                      rfl::Field<"cols_", std::vector<ViewCol>>,
                      rfl::Field<"force_", bool>>;

  /// The command used to transform a view to Arrow.
  using ViewToArrowOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"View.to_arrow">>,
                      rfl::Field<"view_", DataFrameOrView>>;

  /// The command used to transform a view to CSV.
  using ViewToCSVOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"View.to_csv">>,
      rfl::Field<"view_", DataFrameOrView>, rfl::Field<"fname_", std::string>,
      rfl::Field<"batch_size_", size_t>, rfl::Field<"quotechar_", std::string>,
      rfl::Field<"sep_", std::string>>;

  /// The command used to write a view into the database.
  using ViewToDBOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"View.to_db">>,
                      rfl::Field<"view_", DataFrameOrView>,
                      rfl::Field<"conn_id_", std::string>,
                      rfl::Field<"table_name_", std::string>>;

  /// The command used to transform a view to parquet.
  using ViewToParquetOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"View.to_parquet">>,
                      rfl::Field<"view_", DataFrameOrView>,
                      rfl::Field<"fname_", std::string>,
                      rfl::Field<"compression_", std::string>>;

  using NamedTupleType =
      rfl::TaggedUnion<"type_", GetViewContentOp, GetViewNRowsOp, ViewToArrowOp,
                       ViewToCSVOp, ViewToDBOp, ViewToParquetOp>;

  using InputVarType = typename json::Reader::InputVarType;

  static ViewCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_VIEWCOMMAND_HPP_
