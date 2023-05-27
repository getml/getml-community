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
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "fct/TaggedUnion.hpp"
#include "json/json.hpp"

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
  using AddFloatColumnOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"DataFrame.add_column">>,
      fct::Field<"col_", FloatColumnOrFloatColumnView>,
      fct::Field<"df_name_", std::string>, fct::Field<"name_", std::string>,
      fct::Field<"role_", std::string>, fct::Field<"unit_", std::string>>;

  /// The command used to add a string column
  using AddStringColumnOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"DataFrame.add_categorical_column">>,
      fct::Field<"col_", StringColumnOrStringColumnView>,
      fct::Field<"df_name_", std::string>, fct::Field<"name_", std::string>,
      fct::Field<"role_", std::string>, fct::Field<"unit_", std::string>>;

  /// The command used to aggregate a column
  using AggregationOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"FloatColumn.aggregate",
                                       "StringColumn.aggregate">>,
      fct::Field<"aggregation_", Aggregation>>;

  /// The command used to append data received from the client to an existing
  /// data frame.
  using AppendToDataFrameOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.append">>,
                      fct::Field<"name_", std::string>>;

  /// The command used to calculate the plot for a string column.
  using CalcCategoricalColumnPlotOp = fct::NamedTuple<
      fct::Field<"type_",
                 fct::Literal<"DataFrame.calc_categorical_column_plots">>,
      fct::Field<"df_name_", std::string>, fct::Field<"name_", std::string>,
      fct::Field<"num_bins_", size_t>, fct::Field<"role_", std::string>,
      fct::Field<"target_name_", std::string>,
      fct::Field<"target_role_", std::string>>;

  /// The command used to calculate the plot for a float column.
  using CalcColumnPlotOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"DataFrame.calc_column_plots">>,
      fct::Field<"df_name_", std::string>, fct::Field<"name_", std::string>,
      fct::Field<"num_bins_", size_t>, fct::Field<"role_", std::string>,
      fct::Field<"target_name_", std::string>,
      fct::Field<"target_role_", std::string>>;

  /// The command used to concatenate data frames.
  using ConcatDataFramesOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.concat">>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"data_frames_", std::vector<DataFrameOrView>>>;

  /// The command used to add a float column sent by the client
  using FloatColumnOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"FloatColumn">>,
                      fct::Field<"df_name_", std::string>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"role_", std::string>>;

  /// The command used to freeze a data frame, making it immutable.
  using FreezeDataFrameOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.freeze">>,
                      fct::Field<"name_", std::string>>;

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
  using GetBooleanColumnNRowsOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"BooleanColumn.nrows">>,
                      fct::Field<"col_", BooleanColumnView>>;

  /// The command used to retrieve the number of rows of a boolean column.
  using GetDataFrameOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.get">>>;

  /// The command used to retrieve the content of a data frame.
  using GetDataFrameContentOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"DataFrame.get_content">>,
      fct::Field<"draw_", Int>, fct::Field<"length_", Int>,
      fct::Field<"name_", std::string>, fct::Field<"start_", Int>>;

  /// The command used to retrieve the HTML representation of a data frame.
  using GetDataFrameHTMLOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.get_html">>,
                      fct::Field<"max_rows_", size_t>,
                      fct::Field<"border_", size_t>,
                      fct::Field<"name_", std::string>>;

  /// The command used to retrieve the string representation of a data frame.
  using GetDataFrameStringOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.get_string">>,
                      fct::Field<"name_", std::string>>;

  /// The command used to retrieve the the number of bytes in a data frame.
  using GetDataFrameNBytesOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.nbytes">>,
                      fct::Field<"name_", std::string>>;

  /// The command used to retrieve the the number of rows in a data frame.
  using GetDataFrameNRowsOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.nrows">>,
                      fct::Field<"name_", std::string>>;

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
  using GetFloatColumnNRowsOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"FloatColumn.nrows">>,
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
  using GetStringColumnNRowsOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"StringColumn.nrows">>,
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

  /// The command used to retrieve the datetime of the last change on a data
  /// frame.
  using LastChangeOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"DataFrame.last_change">>,
      fct::Field<"name_", std::string>>;

  /// The command used to refresh a data frame.
  using RefreshDataFrameOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.refresh">>,
                      fct::Field<"name_", std::string>>;

  /// The command used to remove a column from a data frame.
  using RemoveColumnOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"DataFrame.remove_column">>,
      fct::Field<"df_name_", std::string>, fct::Field<"name_", std::string>>;

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

  /// The command used to get summary statistics of a data frame.
  using SummarizeDataFrameOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.summarize">>,
                      fct::Field<"name_", std::string>>;

  /// The command used to transform a data frame to Arrow.
  using ToArrowOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.to_arrow">>,
                      fct::Field<"name_", std::string>>;

  /// The command used to transform a data frame to CSV.
  using ToCSVOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"DataFrame.to_csv">>,
      fct::Field<"name_", std::string>, fct::Field<"fname_", std::string>,
      fct::Field<"batch_size_", size_t>, fct::Field<"quotechar_", std::string>,
      fct::Field<"sep_", std::string>>;

  /// The command used to write a data frame into the database.
  using ToDBOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.to_db">>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"conn_id_", std::string>,
                      fct::Field<"table_name_", std::string>>;

  /// The command used to transform a data frame to parquet.
  using ToParquetOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.to_parquet">>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"fname_", std::string>,
                      fct::Field<"compression_", std::string>>;

  using NamedTupleType = fct::TaggedUnion<
      "type_", AddFloatColumnOp, AddStringColumnOp, AppendToDataFrameOp,
      CalcCategoricalColumnPlotOp, CalcColumnPlotOp, ConcatDataFramesOp,
      FreezeDataFrameOp, GetDataFrameOp, GetDataFrameContentOp,
      GetDataFrameHTMLOp, GetDataFrameStringOp, GetDataFrameNBytesOp,
      GetDataFrameNRowsOp, LastChangeOp, RefreshDataFrameOp, RemoveColumnOp,
      SummarizeDataFrameOp, ToArrowOp, ToCSVOp, ToDBOp, ToParquetOp>;

  using InputVarType = typename json::JSONReader::InputVarType;

  static DataFrameCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_DATAFRAMECOMMAND_HPP_
