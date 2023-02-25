// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_PROJECTCOMMAND_HPP_
#define COMMANDS_PROJECTCOMMAND_HPP_

#include <string>

#include "commands/Int.hpp"
#include "commands/Pipeline.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "fct/TaggedUnion.hpp"

namespace commands {

/// Any command to be handled by the ProjectManager.
struct ProjectCommand {
  /// The command to add a data frame from arrow.
  using AddDfFromArrowOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.from_arrow">>,
                      fct::Field<"append_", bool>,
                      fct::Field<"name_", std::string>>;

  /// The command to add a data frame from CSV.
  using AddDfFromCSVOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.read_csv">>,
                      fct::Field<"append_", bool>,
                      fct::Field<"name_", std::string>>;

  /// The command to add a data frame from a database.
  using AddDfFromDBOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.from_db">>,
                      fct::Field<"append_", bool>,
                      fct::Field<"name_", std::string>>;

  /// The command to add a data frame from JSON.
  using AddDfFromJSONOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.from_json">>,
                      fct::Field<"append_", bool>,
                      fct::Field<"name_", std::string>>;

  /// The command to add a data frame from parquet.
  using AddDfFromParquetOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"DataFrame.read_parquet">>,
      fct::Field<"append_", bool>, fct::Field<"name_", std::string>>;

  /// The command to add a data frame from JSON.
  using AddDfFromQueryOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.from_query">>,
                      fct::Field<"append_", bool>,
                      fct::Field<"name_", std::string>>;

  /// The command to add a data frame from a View.
  using AddDfFromViewOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.from_view">>,
                      fct::Field<"append_", bool>,
                      fct::Field<"name_", std::string>>;

  /// The command to add a data frame from a View.
  using CopyPipelineOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Pipeline.copy">>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"other_", std::string>>;

  /// The command to delete a data frame.
  using DeleteDataFrameOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"DataFrame.delete">>,
                      fct::Field<"name_", std::string>>;

  /// The command to delete a pipeline.
  using DeletePipelineOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Pipeline.delete">>,
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

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_PROJECTCOMMAND_HPP_
