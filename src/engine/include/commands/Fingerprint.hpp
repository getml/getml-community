// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_FINGERPRINT_HPP_
#define COMMANDS_FINGERPRINT_HPP_

#include <variant>
#include <vector>

#include "commands/DataFrameOrView.hpp"
#include "commands/DataModel.hpp"
#include "commands/LinearRegressionHyperparams.hpp"
#include "commands/LogisticRegressionHyperparams.hpp"
#include "commands/Preprocessor.hpp"
#include "commands/XGBoostHyperparams.hpp"
#include "fastprop/Hyperparameters.hpp"
#include "json/json.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/TaggedUnion.hpp"
#include "rfl/define_named_tuple.hpp"
#include "rfl/define_tagged_union.hpp"
#include "rfl/define_variant.hpp"
#include "rfl/named_tuple_t.hpp"

namespace commands {

struct Fingerprint {
  // -----------------------------

  /// This needs to be added to every fingerprint.
  using Dependencies =
      rfl::NamedTuple<rfl::Field<"dependencies_", std::vector<Fingerprint>>>;

  // -----------------------------
  // Data frame fingerprints

  /// For retrieving ordinary data frames that were neither created by a view
  /// nor a pipeline.
  using OrdinaryDataFrame =
      rfl::NamedTuple<rfl::Field<"name_", std::string>,
                      rfl::Field<"last_change_", std::string>>;

  /// For retrieving data frames that are the results of entire pipelines (and
  /// thus contain features).
  using PipelineBuildHistory = rfl::define_named_tuple_t<
      Dependencies,
      rfl::Field<"df_fingerprints_", std::vector<commands::Fingerprint>>>;

  using DataFrameFingerprint =
      std::variant<typename DataFrameOrView::ViewOp, OrdinaryDataFrame,
                   rfl::Ref<const DataModel>, PipelineBuildHistory>;

  // -----------------------------
  // Preprocessors

  /// The fingerprint for a CategoryTrimmer.
  using CategoryTrimmerFingerprint = rfl::define_named_tuple_t<
      Dependencies,
      rfl::named_tuple_t<typename Preprocessor::CategoryTrimmerOp>>;

  /// The fingerprint for an EmailDomain preprocessor.
  using EMailDomainFingerprint = rfl::define_named_tuple_t<
      Dependencies, rfl::named_tuple_t<typename Preprocessor::EMailDomainOp>>;

  /// The fingerprint for an Imputation preprocessor.
  using ImputationFingerprint = rfl::define_named_tuple_t<
      Dependencies, rfl::named_tuple_t<typename Preprocessor::ImputationOp>>;

  /// The fingerprint for a Seasonal preprocessor.
  using SeasonalFingerprint = rfl::define_named_tuple_t<
      Dependencies, rfl::named_tuple_t<typename Preprocessor::SeasonalOp>>;

  /// The fingerprint for a Substring preprocessor.
  using SubstringFingerprint = rfl::define_named_tuple_t<
      Dependencies, rfl::named_tuple_t<typename Preprocessor::SubstringOp>>;

  /// The fingerprint for a TextFieldSplitter preprocessor.
  using TextFieldSplitterFingerprint = rfl::define_named_tuple_t<
      Dependencies,
      rfl::named_tuple_t<typename Preprocessor::TextFieldSplitterOp>>;

  using PreprocessorFingerprint =
      std::variant<CategoryTrimmerFingerprint, EMailDomainFingerprint,
                   ImputationFingerprint, SeasonalFingerprint,
                   SubstringFingerprint, TextFieldSplitterFingerprint>;

  // -----------------------------

  /// FeatureLearner fingerprints also require this.
  using OtherFLRequirements = rfl::NamedTuple<
      rfl::Field<"peripheral_", rfl::Ref<const std::vector<std::string>>>,
      rfl::Field<"placeholder_", rfl::Ref<const helpers::Placeholder>>,
      rfl::Field<"target_num_", Int>>;

  /// The fingerprint for a FastProp feature learner.
  using FastPropFingerprint = rfl::define_named_tuple_t<
      typename fastprop::Hyperparameters::NamedTupleType, Dependencies,
      OtherFLRequirements>;

  using FeatureLearnerFingerprint = std::variant<FastPropFingerprint>;

  // -----------------------------
  // Predictors

  /// There are different predictors for every target, so the target number must
  /// also be supported as a dependency.
  using TargetNumber = rfl::NamedTuple<rfl::Field<"target_num_", size_t>>;

  /// The predictors also require information about the exact columns inserted
  /// into them.
  using OtherPredRequirements = rfl::NamedTuple<
      rfl::Field<"autofeatures_", std::vector<std::vector<size_t>>>,
      rfl::Field<"categorical_colnames_", std::vector<std::string>>,
      rfl::Field<"numerical_colnames_", std::vector<std::string>>>;

  /// The fingerprint for a LinearRegression.
  using LinearRegressionFingerprint = rfl::define_named_tuple_t<
      typename LinearRegressionHyperparams::NamedTupleType, Dependencies,
      OtherPredRequirements>;

  /// The fingerprint for a LinearRegression.
  using LogisticRegressionFingerprint = rfl::define_named_tuple_t<
      typename LogisticRegressionHyperparams::NamedTupleType, Dependencies,
      OtherPredRequirements>;

  /// The fingerprint for an XGBoostPredictor.
  using XGBoostFingerprint =
      rfl::define_named_tuple_t<typename XGBoostHyperparams::NamedTupleType,
                                Dependencies, OtherPredRequirements>;

  using PredictorFingerprint =
      std::variant<LinearRegressionFingerprint, LogisticRegressionFingerprint,
                   XGBoostFingerprint, TargetNumber>;

  // -----------------------------

  using NamedTupleType =
      rfl::define_variant_t<DataFrameFingerprint, PreprocessorFingerprint,
                            FeatureLearnerFingerprint, PredictorFingerprint>;

  // -----------------------------

  Fingerprint(const NamedTupleType& _val) : val_(_val) {}

  static Fingerprint from_json(const std::string& _json_str);

  using InputVarType = typename json::Reader::InputVarType;

  static Fingerprint from_json_obj(const InputVarType& _json_obj);

  std::string to_json() const;

  ~Fingerprint() = default;

  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_FINGERPRINT_HPP_
