// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_PIPELINECOMMAND_HPP_
#define COMMANDS_PIPELINECOMMAND_HPP_

#include <string>

#include "commands/DataFramesOrViews.hpp"
#include "commands/Int.hpp"
#include "rfl/Field.hpp"
#include "rfl/Flatten.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"
#include "rfl/TaggedUnion.hpp"
#include "rfl/json.hpp"
#include "transpilation/TranspilationParams.hpp"

namespace commands {

/// Any command to be handled by the PipelineManager.
struct PipelineCommand {
  struct CheckOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.check">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"population_df_", DataFrameOrView> population_df;
    rfl::Field<"peripheral_dfs_", std::vector<DataFrameOrView>> peripheral_dfs;
    rfl::Field<"validation_df_", std::optional<DataFrameOrView>> validation_df;
  };

  struct ColumnImportancesOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.column_importances">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"target_num_", Int> target_num;
  };

  struct DeployOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.deploy">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"deploy_", bool> deploy;
  };

  struct FeatureCorrelationsOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.feature_correlations">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"target_num_", Int> target_num;
  };

  struct FeatureImportancesOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.feature_importances">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"target_num_", Int> target_num;
  };

  struct FitOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.fit">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"population_df_", DataFrameOrView> population_df;
    rfl::Field<"peripheral_dfs_", std::vector<DataFrameOrView>> peripheral_dfs;
    rfl::Field<"validation_df_", std::optional<DataFrameOrView>> validation_df;
  };

  struct LiftCurveOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.lift_curve">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"target_num_", Int> target_num;
  };

  struct PrecisionRecallCurveOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.precision_recall_curve">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"target_num_", Int> target_num;
  };

  struct RefreshOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.refresh">> type;
    rfl::Field<"name_", std::string> name;
  };

  struct RefreshAllOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.refresh_all">> type;
  };

  struct ROCCurveOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.roc_curve">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"target_num_", Int> target_num;
  };

  struct ToSQLOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.to_sql">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"targets_", bool> targets;
    rfl::Field<"subfeatures_", bool> subfeatures;
    rfl::Field<"size_threshold_", std::optional<size_t>> size_threshold;
    rfl::Flatten<transpilation::TranspilationParams> transpilation_params;
  };

  struct TransformOp {
    rfl::Field<"type_", rfl::Literal<"Pipeline.transform">> type;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"http_request_", bool> http_request;
  };

  using ReflectionType =
      rfl::TaggedUnion<"type_", CheckOp, ColumnImportancesOp, DeployOp,
                       FeatureCorrelationsOp, FeatureImportancesOp, FitOp,
                       LiftCurveOp, PrecisionRecallCurveOp, RefreshOp,
                       RefreshAllOp, ROCCurveOp, ToSQLOp, TransformOp>;

  using InputVarType = typename rfl::json::Reader::InputVarType;

  static PipelineCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  ReflectionType val_;
};

}  // namespace commands

#endif  // COMMANDS_PIPELINECOMMAND_HPP_
