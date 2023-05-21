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

#include "commands/DataFrameOrView.hpp"
#include "commands/Int.hpp"
#include "commands/Pipeline.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "fct/TaggedUnion.hpp"
#include "fct/define_named_tuple.hpp"
#include "helpers/Schema.hpp"

namespace commands {

/// Any command to be handled by the ProjectManager.
struct ProjectCommand {
  /// The command to add a data frame from arrow.
  using AddDfFromArrowOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"DataFrame.from_arrow">>,
      typename helpers::Schema::NamedTupleType, fct::Field<"append_", bool>>;

  /// The command to add a data frame from CSV.
  using AddDfFromCSVOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"DataFrame.read_csv">>,
      typename helpers::Schema::NamedTupleType, fct::Field<"append_", bool>,
      fct::Field<"colnames_", std::optional<std::vector<std::string>>>,
      fct::Field<"fnames_", std::vector<std::string>>,
      fct::Field<"num_lines_read_", size_t>,
      fct::Field<"quotechar_", std::string>, fct::Field<"sep_", std::string>,
      fct::Field<"skip_", size_t>,
      fct::Field<"time_formats_", std::vector<std::string>>>;

  /// The command to add a data frame from a database.
  using AddDfFromDBOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"DataFrame.from_db">>,
      typename helpers::Schema::NamedTupleType, fct::Field<"append_", bool>,
      fct::Field<"conn_id_", std::string>,
      fct::Field<"table_name_", std::string>>;

  /// The command to add a data frame from JSON.
  using AddDfFromJSONOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"DataFrame.from_json">>,
      typename helpers::Schema::NamedTupleType, fct::Field<"append_", bool>,
      fct::Field<"time_formats_", std::vector<std::string>>>;

  /// The command to add a data frame from parquet.
  using AddDfFromParquetOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"DataFrame.read_parquet">>,
      typename helpers::Schema::NamedTupleType, fct::Field<"append_", bool>,
      fct::Field<"fname_", std::string>>;

  /// The command to add a data frame from JSON.
  using AddDfFromQueryOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"DataFrame.from_query">>,
      typename helpers::Schema::NamedTupleType, fct::Field<"append_", bool>,
      fct::Field<"conn_id_", std::string>, fct::Field<"query_", std::string>>;

  /// The command to add a data frame from a View.
  using AddDfFromViewOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"DataFrame.from_view">>,
      fct::Field<"append_", bool>, fct::Field<"name_", std::string>,
      fct::Field<"view_", DataFrameOrView>>;

  /// The command to add a data frame from a View.
  using CopyPipelineOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Pipeline.copy">>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"other_", std::string>>;

  /// The command to delete a data frame.
  using DeleteDataFrameOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.delete">>,
                      fct::Field<"mem_only_", bool>,
                      fct::Field<"name_", std::string>>;

  /// The command to delete a pipeline.
  using DeletePipelineOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Pipeline.delete">>,
                      fct::Field<"mem_only_", bool>,
                      fct::Field<"name_", std::string>>;

  /// The command to delete a project.
  using DeleteProjectOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"delete_project">>,
                      fct::Field<"name_", std::string>>;

  /// The command to list all data frames.
  using ListDfsOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"list_data_frames">>>;

  /// The command to list all pipelines.
  using ListPipelinesOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"list_pipelines">>>;

  /// The command to list all pipelines.
  using ListProjectsOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"list_projects">>>;

  /// The command to load a data contaner.
  using LoadDataContainerOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataContainer.load">>,
                      fct::Field<"name_", std::string>>;

  /// The command to load a data frame.
  using LoadDfOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.load">>,
                      fct::Field<"name_", std::string>>;

  /// The command to load a data frame.
  using LoadPipelineOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Pipeline.load">>,
                      fct::Field<"name_", std::string>>;

  /// The command to create a new pipeline.
  using PipelineOp = Pipeline;

  /// The command to send the project name.
  using ProjectNameOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"project_name">>>;

  /// The command to load a data contaner.
  using SaveDataContainerOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataContainer.save">>,
                      fct::Field<"name_", std::string>>;

  /// The command to save a data frame.
  using SaveDfOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.save">>,
                      fct::Field<"name_", std::string>>;

  /// The command to save a pipeline.
  using SavePipelineOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Pipeline.save">>,
                      fct::Field<"name_", std::string>>;

  /// The command to get the temp_dir.
  using TempDirOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"temp_dir">>>;

  using NamedTupleType = fct::TaggedUnion<
      "type_", AddDfFromArrowOp, AddDfFromCSVOp, AddDfFromDBOp, AddDfFromJSONOp,
      AddDfFromParquetOp, AddDfFromQueryOp, AddDfFromViewOp, CopyPipelineOp,
      DeleteDataFrameOp, DeletePipelineOp, DeleteProjectOp, ListDfsOp,
      ListPipelinesOp, ListProjectsOp, LoadDataContainerOp, LoadDfOp,
      LoadPipelineOp, PipelineOp, ProjectNameOp, SaveDataContainerOp, SaveDfOp,
      SavePipelineOp, TempDirOp>;

  using InputVarType = typename json::JSONParser::InputVarType;

  static ProjectCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_PROJECTCOMMAND_HPP_
