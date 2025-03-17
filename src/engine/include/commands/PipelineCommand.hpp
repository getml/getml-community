// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_PIPELINECOMMAND_HPP_
#define COMMANDS_PIPELINECOMMAND_HPP_

#include "commands/DataFrameOrView.hpp"
#include "commands/Int.hpp"
#include "transpilation/TranspilationParams.hpp"

#include <rfl/Field.hpp>
#include <rfl/Flatten.hpp>
#include <rfl/Literal.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>
#include <rfl/TaggedUnion.hpp>
#include <rfl/json/Reader.hpp>

#include <string>

namespace commands {

/// Any command to be handled by the PipelineManager.
struct PipelineCommand {
  struct CheckOp {
    using Tag = rfl::Literal<"Pipeline.check">;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"population_df_", DataFrameOrView> population_df;
    rfl::Field<"peripheral_dfs_", std::vector<DataFrameOrView>> peripheral_dfs;
    rfl::Field<"validation_df_", std::optional<DataFrameOrView>> validation_df;
  };

  struct ColumnImportancesOp {
    using Tag = rfl::Literal<"Pipeline.column_importances">;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"target_num_", Int> target_num;
  };

  struct DeployOp {
    using Tag = rfl::Literal<"Pipeline.deploy">;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"deploy_", bool> deploy;
  };

  struct FeatureCorrelationsOp {
    using Tag = rfl::Literal<"Pipeline.feature_correlations">;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"target_num_", Int> target_num;
  };

  struct FeatureImportancesOp {
    using Tag = rfl::Literal<"Pipeline.feature_importances">;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"target_num_", Int> target_num;
  };

  struct FitOp {
    using Tag = rfl::Literal<"Pipeline.fit">;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"population_df_", DataFrameOrView> population_df;
    rfl::Field<"peripheral_dfs_", std::vector<DataFrameOrView>> peripheral_dfs;
    rfl::Field<"validation_df_", std::optional<DataFrameOrView>> validation_df;
  };

  struct LiftCurveOp {
    using Tag = rfl::Literal<"Pipeline.lift_curve">;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"target_num_", Int> target_num;
  };

  struct PrecisionRecallCurveOp {
    using Tag = rfl::Literal<"Pipeline.precision_recall_curve">;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"target_num_", Int> target_num;
  };

  struct RefreshOp {
    using Tag = rfl::Literal<"Pipeline.refresh">;
    rfl::Field<"name_", std::string> name;
  };

  struct RefreshAllOp {
    using Tag = rfl::Literal<"Pipeline.refresh_all">;
    rfl::Field<"dummy_", std::optional<int>> dummy;
  };

  struct ROCCurveOp {
    using Tag = rfl::Literal<"Pipeline.roc_curve">;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"target_num_", Int> target_num;
  };

  struct ToSQLOp {
    using Tag = rfl::Literal<"Pipeline.to_sql">;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"targets_", bool> targets;
    rfl::Field<"subfeatures_", bool> subfeatures;
    rfl::Field<"size_threshold_", std::optional<size_t>> size_threshold;
    rfl::Flatten<transpilation::TranspilationParams> transpilation_params;
  };

  struct TransformOp {
    using Tag = rfl::Literal<"Pipeline.transform">;
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
