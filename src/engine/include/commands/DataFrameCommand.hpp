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
      std::variant<typename FloatColumnOrFloatColumnView::ReflectionType,
                   typename StringColumnOrStringColumnView::ReflectionType>;

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
  struct AddFloatColumnOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.add_column">> type;
    rfl::Field<"col_", FloatColumnOrFloatColumnView> col;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
    rfl::Field<"unit_", std::string> unit;
  };

  /// The command used to add a string column
  struct AddStringColumnOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.add_categorical_column">> type;
    rfl::Field<"col_", StringColumnOrStringColumnView> col;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
    rfl::Field<"unit_", std::string> unit;
  };

  /// The command used to aggregate a column
  struct AggregationOp {
    rfl::Field<"type_",
               rfl::Literal<"FloatColumn.aggregate", "StringColumn.aggregate">>
        type;
    rfl::Field<"aggregation_", Aggregation> aggregation;
  };

  /// The command used to append data received from the client to an existing
  /// data frame.
  struct AppendToDataFrameOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.append">> type;
    rfl::Field<"name_", std::string> name;
  };

  /// The command used to calculate the plot for a string column.
  struct CalcCategoricalColumnPlotOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.calc_categorical_column_plots">>
        type;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"num_bins_", size_t> num_bins;
    rfl::Field<"role_", std::string> role;
    rfl::Field<"target_name_", std::string> target_name;
    rfl::Field<"target_role_", std::string> target_role;
  };

  /// The command used to calculate the plot for a float column.
  struct CalcColumnPlotOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.calc_column_plots">> type;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"num_bins_", size_t> num_bins;
    rfl::Field<"role_", std::string> role;
    rfl::Field<"target_name_", std::string> target_name;
    rfl::Field<"target_role_", std::string> target_role;
  };

  /// The command used to concatenate data frames.
  struct ConcatDataFramesOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.concat">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"data_frames_", std::vector<DataFrameOrView>> data_frames;
  };

  /// The command used to add a float column sent by the client
  struct FloatColumnOp {
    rfl::Field<"type_", rfl::Literal<"FloatColumn">> type;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
  };

  /// The command used to freeze a data frame, making it immutable.
  struct FreezeDataFrameOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.freeze">> type;
    rfl::Field<"name_", std::string> name;
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
    rfl::Field<"type_", rfl::Literal<"BooleanColumn.nrows">> type;
    rfl::Field<"col_", BooleanColumnView> col;
  };

  /// The command used to retrieve the number of rows of a boolean column.
  struct GetDataFrameOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.get">> type;
  };

  /// The command used to retrieve the content of a data frame.
  struct GetDataFrameContentOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.get_content">> type;
    rfl::Field<"draw_", Int> draw;
    rfl::Field<"length_", Int> length;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"start_", Int> start;
  };

  /// The command used to retrieve the HTML representation of a data frame.
  struct GetDataFrameHTMLOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.get_html">> type;
    rfl::Field<"max_rows_", size_t> max_rows;
    rfl::Field<"border_", size_t> border;
    rfl::Field<"name_", std::string> name;
  };

  /// The command used to retrieve the string representation of a data frame.
  struct GetDataFrameStringOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.get_string">> type;
    rfl::Field<"name_", std::string> name;
  };

  /// The command used to retrieve the the number of bytes in a data frame.
  struct GetDataFrameNBytesOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.nbytes">> type;
    rfl::Field<"name_", std::string> name;
  };

  /// The command used to retrieve the the number of rows in a data frame.
  struct GetDataFrameNRowsOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.nrows">> type;
    rfl::Field<"name_", std::string> name;
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
    rfl::Field<"type_", rfl::Literal<"FloatColumn.nrows">> type;
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
    rfl::Field<"type_", rfl::Literal<"StringColumn.nrows">> type;
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

  /// The command used to retrieve the datetime of the last change on a data
  /// frame.
  struct LastChangeOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.last_change">> type;
    rfl::Field<"name_", std::string> name;
  };

  /// The command used to refresh a data frame.
  struct RefreshDataFrameOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.refresh">> type;
    rfl::Field<"name_", std::string> name;
  };

  /// The command used to remove a column from a data frame.
  struct RemoveColumnOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.remove_column">> type;
    rfl::Field<"df_name_", std::string> df_name;
    rfl::Field<"name_", std::string> name;
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

  /// The command used to get summary statistics of a data frame.
  struct SummarizeDataFrameOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.summarize">> type;
    rfl::Field<"name_", std::string> name;
  };

  /// The command used to transform a data frame to Arrow.
  struct ToArrowOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.to_arrow">> type;
    rfl::Field<"name_", std::string> name;
  };

  /// The command used to transform a data frame to CSV.
  struct ToCSVOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.to_csv">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"fname_", std::string> fname;
    rfl::Field<"batch_size_", size_t> batch_size;
    rfl::Field<"quotechar_", std::string> quotechar;
    rfl::Field<"sep_", std::string> sep;
  };

  /// The command used to write a data frame into the database.
  struct ToDBOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.to_db">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"table_name_", std::string> table_name;
  };

  /// The command used to transform a data frame to parquet.
  struct ToParquetOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.to_parquet">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"fname_", std::string> fname;
    rfl::Field<"compression_", std::string> compression;
  };

  using ReflectionType = rfl::TaggedUnion<
      "type_", AddFloatColumnOp, AddStringColumnOp, AppendToDataFrameOp,
      CalcCategoricalColumnPlotOp, CalcColumnPlotOp, ConcatDataFramesOp,
      FreezeDataFrameOp, GetDataFrameOp, GetDataFrameContentOp,
      GetDataFrameHTMLOp, GetDataFrameStringOp, GetDataFrameNBytesOp,
      GetDataFrameNRowsOp, LastChangeOp, RefreshDataFrameOp, RemoveColumnOp,
      SummarizeDataFrameOp, ToArrowOp, ToCSVOp, ToDBOp, ToParquetOp>;

  using InputVarType = typename json::Reader::InputVarType;

  static DataFrameCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  ReflectionType val_;
};

}  // namespace commands

#endif  // COMMANDS_DATAFRAMECOMMAND_HPP_
