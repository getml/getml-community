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
#include "json/json.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"
#include "rfl/TaggedUnion.hpp"
#include "rfl/define_named_tuple.hpp"
#include "transpilation/TranspilationParams.hpp"

namespace commands {

/// Any command to be handled by the PipelineManager.
struct PipelineCommand {
  using CheckOp = rfl::define_named_tuple_t<
      rfl::Field<"type_", rfl::Literal<"Pipeline.check">>,
      rfl::Field<"name_", std::string>, DataFramesOrViews>;

  using ColumnImportancesOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"Pipeline.column_importances">>,
      rfl::Field<"name_", std::string>, rfl::Field<"target_num_", Int>>;

  using DeployOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Pipeline.deploy">>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"deploy_", bool>>;

  using FeatureCorrelationsOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"Pipeline.feature_correlations">>,
      rfl::Field<"name_", std::string>, rfl::Field<"target_num_", Int>>;

  using FeatureImportancesOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"Pipeline.feature_importances">>,
      rfl::Field<"name_", std::string>, rfl::Field<"target_num_", Int>>;

  using FitOp = rfl::define_named_tuple_t<
      rfl::Field<"type_", rfl::Literal<"Pipeline.fit">>,
      rfl::Field<"name_", std::string>, DataFramesOrViews>;

  using LiftCurveOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Pipeline.lift_curve">>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"target_num_", Int>>;

  using PrecisionRecallCurveOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"Pipeline.precision_recall_curve">>,
      rfl::Field<"name_", std::string>, rfl::Field<"target_num_", Int>>;

  using RefreshOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Pipeline.refresh">>,
                      rfl::Field<"name_", std::string>>;

  using RefreshAllOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"Pipeline.refresh_all">>>;

  using ROCCurveOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Pipeline.roc_curve">>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"target_num_", Int>>;

  using ToSQLOp = rfl::define_named_tuple_t<
      rfl::Field<"type_", rfl::Literal<"Pipeline.to_sql">>,
      rfl::Field<"name_", std::string>, rfl::Field<"targets_", bool>,
      rfl::Field<"subfeatures_", bool>,
      rfl::Field<"size_threshold_", std::optional<size_t>>,
      typename transpilation::TranspilationParams::NamedTupleType>;

  using TransformOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Pipeline.transform">>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"http_request_", bool>>;

  using NamedTupleType =
      rfl::TaggedUnion<"type_", CheckOp, ColumnImportancesOp, DeployOp,
                       FeatureCorrelationsOp, FeatureImportancesOp, FitOp,
                       LiftCurveOp, PrecisionRecallCurveOp, RefreshOp,
                       RefreshAllOp, ROCCurveOp, ToSQLOp, TransformOp>;

  using InputVarType = typename json::Reader::InputVarType;

  static PipelineCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_PIPELINECOMMAND_HPP_
