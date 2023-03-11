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
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/TaggedUnion.hpp"
#include "fct/define_named_tuple.hpp"
#include "fct/define_tagged_union.hpp"

namespace commands {

struct Fingerprint {
  // -----------------------------

  /// This needs to be added to every fingerprint.
  using Dependencies =
      fct::NamedTuple<fct::Field<"dependencies_", std::vector<Fingerprint>>>;

  // -----------------------------
  // Data frame fingerprints

  /// For retrieving ordinary data frames that were neither created by a view
  /// nor a pipeline.
  using OrdinaryDataFrame =
      fct::NamedTuple<fct::Field<"name_", std::string>,
                      fct::Field<"last_change_", std::string>>;

  using DataFrameFingerprint =
      std::variant<typename DataFrameOrView::ViewOp, OrdinaryDataFrame,
                   fct::Ref<const DataModel>>;

  // -----------------------------
  // Preprocessors

  /// The fingerprint for a CategoryTrimmer.
  using CategoryTrimmerFingerprint =
      fct::define_named_tuple_t<Dependencies,
                                typename Preprocessor::CategoryTrimmerOp>;

  /// The fingerprint for an EmailDomain preprocessor.
  using EMailDomainFingerprint =
      fct::define_named_tuple_t<Dependencies,
                                typename Preprocessor::EMailDomainOp>;

  /// The fingerprint for an Imputation preprocessor.
  using ImputationFingerprint =
      fct::define_named_tuple_t<Dependencies,
                                typename Preprocessor::ImputationOp>;

  /// The fingerprint for a Seasonal preprocessor.
  using SeasonalFingerprint =
      fct::define_named_tuple_t<Dependencies,
                                typename Preprocessor::SeasonalOp>;

  /// The fingerprint for a Substring preprocessor.
  using SubstringFingerprint =
      fct::define_named_tuple_t<Dependencies,
                                typename Preprocessor::SubstringOp>;

  /// The fingerprint for a TextFieldSplitter preprocessor.
  using TextFieldSplitterFingerprint =
      fct::define_named_tuple_t<Dependencies,
                                typename Preprocessor::TextFieldSplitterOp>;

  using PreprocessorFingerprint =
      std::variant<CategoryTrimmerFingerprint, EMailDomainFingerprint,
                   ImputationFingerprint, SeasonalFingerprint,
                   SubstringFingerprint, TextFieldSplitterFingerprint>;

  // -----------------------------

  /// FeatureLearner fingerprints also require this.
  using OtherFLRequirements = fct::NamedTuple<
      fct::Field<"peripheral_", fct::Ref<const std::vector<std::string>>>,
      fct::Field<"placeholder_", fct::Ref<const helpers::Placeholder>>,
      fct::Field<"target_num_", Int>>;

  /// The fingerprint for a FastProp feature learner.
  using FastPropFingerprint = fct::define_named_tuple_t<
      typename fastprop::Hyperparameters::NamedTupleType, Dependencies,
      OtherFLRequirements>;

  using FeatureLearnerFingerprint = std::variant<FastPropFingerprint>;

  // -----------------------------
  // Predictors

  /// There are different predictors for every target, so the target number must
  /// also be supported as a dependency.
  using TargetNumber = fct::NamedTuple<fct::Field<"target_num_", size_t>>;

  /// The predictors also require information about the exact columns inserted
  /// into them.
  using OtherPredRequirements = fct::NamedTuple<
      fct::Field<"autofeatures_", std::vector<std::vector<size_t>>>,
      fct::Field<"categorical_colnames_", std::vector<std::string>>,
      fct::Field<"numerical_colnames_", std::vector<std::string>>>;

  /// The fingerprint for a LinearRegression.
  using LinearRegressionFingerprint = fct::define_named_tuple_t<
      typename LinearRegressionHyperparams::NamedTupleType, Dependencies,
      OtherPredRequirements>;

  /// The fingerprint for a LinearRegression.
  using LogisticRegressionFingerprint = fct::define_named_tuple_t<
      typename LogisticRegressionHyperparams::NamedTupleType, Dependencies,
      OtherPredRequirements>;

  /// The fingerprint for an XGBoostPredictor.
  using XGBoostFingerprint =
      fct::define_named_tuple_t<typename XGBoostHyperparams::NamedTupleType,
                                Dependencies, OtherPredRequirements>;

  using PredictorFingerprint =
      std::variant<LinearRegressionFingerprint, LogisticRegressionFingerprint,
                   XGBoostFingerprint, TargetNumber>;

  // -----------------------------

  using NamedTupleType =
      std::variant<DataFrameFingerprint, PreprocessorFingerprint,
                   FeatureLearnerFingerprint, PredictorFingerprint>;

  // -----------------------------

  Fingerprint(const NamedTupleType& _val) : val_(_val) {}

  ~Fingerprint() = default;

  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_FINGERPRINT_HPP_
