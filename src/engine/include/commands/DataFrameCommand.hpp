// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATAFRAMECOMMAND_HPP_
#define COMMANDS_DATAFRAMECOMMAND_HPP_

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

/// Any command to be handled by the DataFrameManager.
struct DataFrameCommand {
  /// Used as a helper for various commands related to views.
  using ViewCol =
      std::variant<typename FloatColumnOrFloatColumnView::NamedTupleType,
                   typename StringColumnOrStringColumnView::NamedTupleType>;

  /// The command to add a data frame from arrow.
  using AddDfFromArrowOp = typename ProjectCommand::AddDfFromArrowOp;

  /// The command to add a data frame from CSV.
  using AddDfFromCSVOp = typename ProjectCommand::AddDfFromCSVOp;

  /// The command to add a data frame from a database.
  using AddDfFromDBOp = typename ProjectCommand::AddDfFromDBOp;

  /// The command to add a data frame from JSON.
  using AddDfFromJSONOp = typename ProjectCommand::AddDfFromJSONOp;

  /// The command to add a data frame from parquet.
  using AddDfFromParquetOp = typename ProjectCommand::AddDfFromParquetOp;

  /// The command to add a data frame from JSON.
  using AddDfFromQueryOp = typename ProjectCommand::AddDfFromQueryOp;

  /// The command to add a data frame from a View.
  using AddDfFromViewOp = typename ProjectCommand::AddDfFromViewOp;

  /// The command used to add a float column
  using AddFloatColumnOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"DataFrame.add_column">>,
      rfl::Field<"col_", FloatColumnOrFloatColumnView>,
      rfl::Field<"df_name_", std::string>, rfl::Field<"name_", std::string>,
      rfl::Field<"role_", std::string>, rfl::Field<"unit_", std::string>>;

  /// The command used to add a string column
  using AddStringColumnOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"DataFrame.add_categorical_column">>,
      rfl::Field<"col_", StringColumnOrStringColumnView>,
      rfl::Field<"df_name_", std::string>, rfl::Field<"name_", std::string>,
      rfl::Field<"role_", std::string>, rfl::Field<"unit_", std::string>>;

  /// The command used to aggregate a column
  using AggregationOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"FloatColumn.aggregate",
                                       "StringColumn.aggregate">>,
      rfl::Field<"aggregation_", Aggregation>>;

  /// The command used to append data received from the client to an existing
  /// data frame.
  using AppendToDataFrameOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.append">>,
                      rfl::Field<"name_", std::string>>;

  /// The command used to calculate the plot for a string column.
  using CalcCategoricalColumnPlotOp = rfl::NamedTuple<
      rfl::Field<"type_",
                 rfl::Literal<"DataFrame.calc_categorical_column_plots">>,
      rfl::Field<"df_name_", std::string>, rfl::Field<"name_", std::string>,
      rfl::Field<"num_bins_", size_t>, rfl::Field<"role_", std::string>,
      rfl::Field<"target_name_", std::string>,
      rfl::Field<"target_role_", std::string>>;

  /// The command used to calculate the plot for a float column.
  using CalcColumnPlotOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"DataFrame.calc_column_plots">>,
      rfl::Field<"df_name_", std::string>, rfl::Field<"name_", std::string>,
      rfl::Field<"num_bins_", size_t>, rfl::Field<"role_", std::string>,
      rfl::Field<"target_name_", std::string>,
      rfl::Field<"target_role_", std::string>>;

  /// The command used to concatenate data frames.
  using ConcatDataFramesOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.concat">>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"data_frames_", std::vector<DataFrameOrView>>>;

  /// The command used to add a float column sent by the client
  using FloatColumnOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"FloatColumn">>,
                      rfl::Field<"df_name_", std::string>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"role_", std::string>>;

  /// The command used to freeze a data frame, making it immutable.
  using FreezeDataFrameOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.freeze">>,
                      rfl::Field<"name_", std::string>>;

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
  using GetBooleanColumnNRowsOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"BooleanColumn.nrows">>,
                      rfl::Field<"col_", BooleanColumnView>>;

  /// The command used to retrieve the number of rows of a boolean column.
  using GetDataFrameOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.get">>>;

  /// The command used to retrieve the content of a data frame.
  using GetDataFrameContentOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"DataFrame.get_content">>,
      rfl::Field<"draw_", Int>, rfl::Field<"length_", Int>,
      rfl::Field<"name_", std::string>, rfl::Field<"start_", Int>>;

  /// The command used to retrieve the HTML representation of a data frame.
  using GetDataFrameHTMLOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.get_html">>,
                      rfl::Field<"max_rows_", size_t>,
                      rfl::Field<"border_", size_t>,
                      rfl::Field<"name_", std::string>>;

  /// The command used to retrieve the string representation of a data frame.
  using GetDataFrameStringOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.get_string">>,
                      rfl::Field<"name_", std::string>>;

  /// The command used to retrieve the the number of bytes in a data frame.
  using GetDataFrameNBytesOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.nbytes">>,
                      rfl::Field<"name_", std::string>>;

  /// The command used to retrieve the the number of rows in a data frame.
  using GetDataFrameNRowsOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.nrows">>,
                      rfl::Field<"name_", std::string>>;

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
  using GetFloatColumnNRowsOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"FloatColumn.nrows">>,
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
  using GetStringColumnNRowsOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"StringColumn.nrows">>,
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

  /// The command used to retrieve the datetime of the last change on a data
  /// frame.
  using LastChangeOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"DataFrame.last_change">>,
      rfl::Field<"name_", std::string>>;

  /// The command used to refresh a data frame.
  using RefreshDataFrameOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.refresh">>,
                      rfl::Field<"name_", std::string>>;

  /// The command used to remove a column from a data frame.
  using RemoveColumnOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"DataFrame.remove_column">>,
      rfl::Field<"df_name_", std::string>, rfl::Field<"name_", std::string>>;

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

  /// The command used to get summary statistics of a data frame.
  using SummarizeDataFrameOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.summarize">>,
                      rfl::Field<"name_", std::string>>;

  /// The command used to transform a data frame to Arrow.
  using ToArrowOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.to_arrow">>,
                      rfl::Field<"name_", std::string>>;

  /// The command used to transform a data frame to CSV.
  using ToCSVOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"DataFrame.to_csv">>,
      rfl::Field<"name_", std::string>, rfl::Field<"fname_", std::string>,
      rfl::Field<"batch_size_", size_t>, rfl::Field<"quotechar_", std::string>,
      rfl::Field<"sep_", std::string>>;

  /// The command used to write a data frame into the database.
  using ToDBOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.to_db">>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"conn_id_", std::string>,
                      rfl::Field<"table_name_", std::string>>;

  /// The command used to transform a data frame to parquet.
  using ToParquetOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.to_parquet">>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"fname_", std::string>,
                      rfl::Field<"compression_", std::string>>;

  using NamedTupleType = rfl::TaggedUnion<
      "type_", AddFloatColumnOp, AddStringColumnOp, AppendToDataFrameOp,
      CalcCategoricalColumnPlotOp, CalcColumnPlotOp, ConcatDataFramesOp,
      FreezeDataFrameOp, GetDataFrameOp, GetDataFrameContentOp,
      GetDataFrameHTMLOp, GetDataFrameStringOp, GetDataFrameNBytesOp,
      GetDataFrameNRowsOp, LastChangeOp, RefreshDataFrameOp, RemoveColumnOp,
      SummarizeDataFrameOp, ToArrowOp, ToCSVOp, ToDBOp, ToParquetOp>;

  using InputVarType = typename json::Reader::InputVarType;

  static DataFrameCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_DATAFRAMECOMMAND_HPP_
