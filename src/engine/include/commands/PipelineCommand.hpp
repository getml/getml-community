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
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "fct/TaggedUnion.hpp"
#include "fct/define_named_tuple.hpp"

namespace commands {

/// Any command to be handled by the PipelineManager.
struct PipelineCommand {
  using CheckOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"Pipeline.check">>,
      fct::Field<"name_", std::string>, DataFramesOrViews>;

  using ColumnImportancesOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"Pipeline.column_importances">>,
      fct::Field<"name_", std::string>, fct::Field<"target_num_", Int>>;

  using DeployOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Pipeline.deploy">>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"deploy_", bool>>;

  using FeatureCorrelationsOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"Pipeline.feature_correlations">>,
      fct::Field<"name_", std::string>, fct::Field<"target_num_", Int>>;

  using FeatureImportancesOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"Pipeline.feature_importances">>,
      fct::Field<"name_", std::string>, fct::Field<"target_num_", Int>>;

  using FitOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"Pipeline.fit">>,
      fct::Field<"name_", std::string>, DataFramesOrViews>;

  using LiftCurveOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Pipeline.lift_curve">>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"target_num_", Int>>;

  using PrecisionRecallCurveOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"Pipeline.precision_recall_curve">>,
      fct::Field<"name_", std::string>, fct::Field<"target_num_", Int>>;

  using RefreshOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Pipeline.refresh">>,
                      fct::Field<"name_", std::string>>;

  using RefreshAllOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"Pipeline.refresh_all">>>;

  using ROCCurveOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Pipeline.roc_curve">>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"target_num_", Int>>;

  using TransformOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"Pipeline.transform">>,
      fct::Field<"name_", std::string>, fct::Field<"table_name_", std::string>,
      fct::Field<"df_name_", std::string>, fct::Field<"score_", bool>,
      fct::Field<"http_request_", bool>, DataFramesOrViews>;

  using NamedTupleType =
      fct::TaggedUnion<"type_", CheckOp, ColumnImportancesOp, DeployOp,
                       FeatureCorrelationsOp, FeatureImportancesOp, FitOp,
                       LiftCurveOp, PrecisionRecallCurveOp, RefreshOp,
                       RefreshAllOp, ROCCurveOp, TransformOp>;

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_PIPELINECOMMAND_HPP_
