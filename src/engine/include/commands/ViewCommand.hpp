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
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "fct/TaggedUnion.hpp"
#include "json/json.hpp"

namespace commands {

/// Any command to be handled by the ViewManager.
struct ViewCommand {
  /// Used as a helper for various commands related to views.
  using ViewCol =
      std::variant<typename FloatColumnOrFloatColumnView::NamedTupleType,
                   typename StringColumnOrStringColumnView::NamedTupleType>;

  /// The command used to retrieve the content of a view.
  using GetViewContentOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"View.get_content">>,
                      fct::Field<"cols_", std::vector<ViewCol>>,
                      fct::Field<"draw_", Int>, fct::Field<"length_", Int>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"start_", Int>>;

  /// The command used to retrieve the number of rows of a view.
  using GetViewNRowsOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"View.get_nrows">>,
                      fct::Field<"cols_", std::vector<ViewCol>>,
                      fct::Field<"force_", bool>>;

  /// The command used to transform a view to Arrow.
  using ViewToArrowOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"View.to_arrow">>,
                      fct::Field<"view_", DataFrameOrView>>;

  /// The command used to transform a view to CSV.
  using ViewToCSVOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"View.to_csv">>,
      fct::Field<"view_", DataFrameOrView>, fct::Field<"fname_", std::string>,
      fct::Field<"batch_size_", size_t>, fct::Field<"quotechar_", std::string>,
      fct::Field<"sep_", std::string>>;

  /// The command used to write a view into the database.
  using ViewToDBOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"View.to_db">>,
                      fct::Field<"view_", DataFrameOrView>,
                      fct::Field<"conn_id_", std::string>,
                      fct::Field<"table_name_", std::string>>;

  /// The command used to transform a view to parquet.
  using ViewToParquetOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"View.to_parquet">>,
                      fct::Field<"view_", DataFrameOrView>,
                      fct::Field<"fname_", std::string>,
                      fct::Field<"compression_", std::string>>;

  using NamedTupleType =
      fct::TaggedUnion<"type_", GetViewContentOp, GetViewNRowsOp, ViewToArrowOp,
                       ViewToCSVOp, ViewToDBOp, ViewToParquetOp>;

  using InputVarType = typename json::JSONParser::InputVarType;

  static ViewCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_VIEWCOMMAND_HPP_
