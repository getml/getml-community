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
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"
#include "rfl/TaggedUnion.hpp"
#include "rfl/define_named_tuple.hpp"

namespace commands {

/// Any command to be handled by the ProjectManager.
struct ProjectCommand {
  /// The command to add a data frame from arrow.
  using AddDfFromArrowOp = rfl::define_named_tuple_t<
      rfl::Field<"type_", rfl::Literal<"DataFrame.from_arrow">>,
      typename helpers::Schema::NamedTupleType, rfl::Field<"append_", bool>>;

  /// The command to add a data frame from CSV.
  using AddDfFromCSVOp = rfl::define_named_tuple_t<
      rfl::Field<"type_", rfl::Literal<"DataFrame.read_csv">>,
      typename helpers::Schema::NamedTupleType, rfl::Field<"append_", bool>,
      rfl::Field<"colnames_", std::optional<std::vector<std::string>>>,
      rfl::Field<"fnames_", std::vector<std::string>>,
      rfl::Field<"num_lines_read_", size_t>,
      rfl::Field<"quotechar_", std::string>, rfl::Field<"sep_", std::string>,
      rfl::Field<"skip_", size_t>,
      rfl::Field<"time_formats_", std::vector<std::string>>>;

  /// The command to add a data frame from a database.
  using AddDfFromDBOp = rfl::define_named_tuple_t<
      rfl::Field<"type_", rfl::Literal<"DataFrame.from_db">>,
      typename helpers::Schema::NamedTupleType, rfl::Field<"append_", bool>,
      rfl::Field<"conn_id_", std::string>,
      rfl::Field<"table_name_", std::string>>;

  /// The command to add a data frame from JSON.
  using AddDfFromJSONOp = rfl::define_named_tuple_t<
      rfl::Field<"type_", rfl::Literal<"DataFrame.from_json">>,
      typename helpers::Schema::NamedTupleType, rfl::Field<"append_", bool>,
      rfl::Field<"time_formats_", std::vector<std::string>>>;

  /// The command to add a data frame from parquet.
  using AddDfFromParquetOp = rfl::define_named_tuple_t<
      rfl::Field<"type_", rfl::Literal<"DataFrame.read_parquet">>,
      typename helpers::Schema::NamedTupleType, rfl::Field<"append_", bool>,
      rfl::Field<"fname_", std::string>>;

  /// The command to add a data frame from JSON.
  using AddDfFromQueryOp = rfl::define_named_tuple_t<
      rfl::Field<"type_", rfl::Literal<"DataFrame.from_query">>,
      typename helpers::Schema::NamedTupleType, rfl::Field<"append_", bool>,
      rfl::Field<"conn_id_", std::string>, rfl::Field<"query_", std::string>>;

  /// The command to add a data frame from a View.
  using AddDfFromViewOp = rfl::define_named_tuple_t<
      rfl::Field<"type_", rfl::Literal<"DataFrame.from_view">>,
      rfl::Field<"append_", bool>, rfl::Field<"name_", std::string>,
      rfl::Field<"view_", DataFrameOrView>>;

  /// The command to add a data frame from a View.
  using CopyPipelineOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Pipeline.copy">>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"other_", std::string>>;

  /// The command to delete a data frame.
  using DeleteDataFrameOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.delete">>,
                      rfl::Field<"mem_only_", bool>,
                      rfl::Field<"name_", std::string>>;

  /// The command to delete a pipeline.
  using DeletePipelineOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Pipeline.delete">>,
                      rfl::Field<"mem_only_", bool>,
                      rfl::Field<"name_", std::string>>;

  /// The command to delete a project.
  using DeleteProjectOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"delete_project">>,
                      rfl::Field<"name_", std::string>>;

  /// The command to list all data frames.
  using ListDfsOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"list_data_frames">>>;

  /// The command to list all pipelines.
  using ListPipelinesOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"list_pipelines">>>;

  /// The command to list all pipelines.
  using ListProjectsOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"list_projects">>>;

  /// The command to load a data contaner.
  using LoadDataContainerOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataContainer.load">>,
                      rfl::Field<"name_", std::string>>;

  /// The command to load a data frame.
  using LoadDfOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.load">>,
                      rfl::Field<"name_", std::string>>;

  /// The command to load a data frame.
  using LoadPipelineOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Pipeline.load">>,
                      rfl::Field<"name_", std::string>>;

  /// The command to create a new pipeline.
  using PipelineOp = Pipeline;

  /// The command to send the project name.
  using ProjectNameOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"project_name">>>;

  /// The command to load a data contaner.
  using SaveDataContainerOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataContainer.save">>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"container_", DataContainer>>;

  /// The command to save a data frame.
  using SaveDfOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"DataFrame.save">>,
                      rfl::Field<"name_", std::string>>;

  /// The command to save a pipeline.
  using SavePipelineOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"Pipeline.save">>,
      rfl::Field<"name_", std::string>,
      rfl::Field<"format_", std::optional<typename helpers::Saver::Format>>>;

  /// The command to get the temp_dir.
  using TempDirOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"temp_dir">>>;

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
