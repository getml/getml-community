// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_PROJECTCOMMAND_HPP_
#define COMMANDS_PROJECTCOMMAND_HPP_

#include <optional>
#include <string>
#include <vector>

#include "commands/DataContainer.hpp"
#include "commands/DataFrameOrView.hpp"
#include "commands/Int.hpp"
#include "commands/Pipeline.hpp"
#include "helpers/Saver.hpp"
#include "helpers/Schema.hpp"
#include "rfl/Field.hpp"
#include "rfl/Flatten.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"
#include "rfl/TaggedUnion.hpp"
#include "rfl/define_named_tuple.hpp"

namespace commands {

/// Any command to be handled by the ProjectManager.
struct ProjectCommand {
  /// The command to add a data frame from arrow.
  struct AddDfFromArrowOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.from_arrow">> type;
    rfl::Flatten<helpers::SchemaImpl> schema;
    rfl::Field<"append_", bool> append;
  };

  /// The command to add a data frame from CSV.
  struct AddDfFromCSVOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.read_csv">> type;
    rfl::Flatten<helpers::SchemaImpl> schema;
    rfl::Field<"append_", bool> append;
    rfl::Field<"colnames_", std::optional<std::vector<std::string>>> colnames;
    rfl::Field<"fnames_", std::vector<std::string>> fnames;
    rfl::Field<"num_lines_read_", size_t> num_lines_read;
    rfl::Field<"quotechar_", std::string> quotechar;
    rfl::Field<"sep_", std::string> sep;
    rfl::Field<"skip_", size_t> skip;
    rfl::Field<"time_formats_", std::vector<std::string>> time_formats;
  };

  /// The command to add a data frame from a database.
  struct AddDfFromDBOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.from_db">> type;
    rfl::Flatten<helpers::SchemaImpl> schema;
    rfl::Field<"append_", bool> append;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"table_name_", std::string> table_name;
  };

  /// The command to add a data frame from JSON.
  struct AddDfFromJSONOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.from_json">> type;
    rfl::Flatten<helpers::SchemaImpl> schema;
    rfl::Field<"append_", bool> append;
    rfl::Field<"time_formats_", std::vector<std::string>> time_formats;
  };

  /// The command to add a data frame from parquet.
  struct AddDfFromParquetOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.read_parquet">> type;
    rfl::Flatten<helpers::SchemaImpl> schema;
    rfl::Field<"append_", bool> append;
    rfl::Field<"fname_", std::string> fname;
  };

  /// The command to add a data frame from JSON.
  struct AddDfFromQueryOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.from_query">> type;
    rfl::Flatten<helpers::SchemaImpl> schema;
    rfl::Field<"append_", bool> append;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"query_", std::string> query;
  };

  /// The command to add a data frame from a View.
  struct AddDfFromViewOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.from_view">> type;
    rfl::Field<"append_", bool> append;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"view_", DataFrameOrView> view;
  };

  /// The command to add a data frame from a View.
  struct CopyPipelineOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.copy">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"other_", std::string> other;
  };

  /// The command to delete a data frame.
  struct DeleteDataFrameOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.delete">> type;
    rfl::Field<"mem_only_", bool> mem_only;
    rfl::Field<"name_", std::string> name;
  };

  /// The command to delete a pipeline.
  struct DeletePipelineOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.delete">> type;
    rfl::Field<"mem_only_", bool> mem_only;
    rfl::Field<"name_", std::string> name;
  };

  /// The command to delete a project.
  struct DeleteProjectOp {
    rfl::Field<"type_", rfl::Literal<"delete_project">> type;
    rfl::Field<"name_", std::string> name;
  };

  /// The command to list all data frames.
  struct ListDfsOp {
    rfl::Field<"type_", rfl::Literal<"list_data_frames">> type;
  };

  /// The command to list all pipelines.
  struct ListPipelinesOp {
    rfl::Field<"type_", rfl::Literal<"list_pipelines">> type;
  };

  /// The command to list all pipelines.
  struct ListProjectsOp {
    rfl::Field<"type_", rfl::Literal<"list_projects">> type;
  };

  /// The command to load a data contaner.
  struct LoadDataContainerOp {
    rfl::Field<"type_", rfl::Literal<"DataContainer.load">> type;
    rfl::Field<"name_", std::string> name;
  };

  /// The command to load a data frame.
  struct LoadDfOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.load">> type;
    rfl::Field<"name_", std::string> name;
  };

  /// The command to load a data frame.
  struct LoadPipelineOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.load">> type;
    rfl::Field<"name_", std::string> name;
  };

  /// The command to create a new pipeline.
  using PipelineOp = Pipeline;

  /// The command to send the project name.
  struct ProjectNameOp {
    rfl::Field<"type_", rfl::Literal<"project_name">> type;
  };

  /// The command to load a data contaner.
  struct SaveDataContainerOp {
    rfl::Field<"type_", rfl::Literal<"DataContainer.save">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"container_", DataContainer> container;
  };

  /// The command to save a data frame.
  struct SaveDfOp {
    rfl::Field<"type_", rfl::Literal<"DataFrame.save">> type;
    rfl::Field<"name_", std::string> name;
  };

  /// The command to save a pipeline.
  struct SavePipelineOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.save">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"format_", std::optional<typename helpers::Saver::Format>>
        format;
  };

  /// The command to get the temp_dir.
  struct TempDirOp {
    rfl::Field<"type_", rfl::Literal<"temp_dir">> type;
  };

  using NamedTupleType = rfl::TaggedUnion<
      "type_", AddDfFromArrowOp, AddDfFromCSVOp, AddDfFromDBOp, AddDfFromJSONOp,
      AddDfFromParquetOp, AddDfFromQueryOp, AddDfFromViewOp, CopyPipelineOp,
      DeleteDataFrameOp, DeletePipelineOp, DeleteProjectOp, ListDfsOp,
      ListPipelinesOp, ListProjectsOp, LoadDataContainerOp, LoadDfOp,
      LoadPipelineOp, PipelineOp, ProjectNameOp, SaveDataContainerOp, SaveDfOp,
      SavePipelineOp, TempDirOp>;

  using InputVarType = typename json::Reader::InputVarType;

  static ProjectCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_PROJECTCOMMAND_HPP_
