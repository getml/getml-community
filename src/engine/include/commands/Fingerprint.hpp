// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_FINGERPRINT_HPP_
#define COMMANDS_FINGERPRINT_HPP_

#include "commands/DataFrameOrView.hpp"
#include "commands/DataModel.hpp"
#include "commands/LinearRegressionHyperparams.hpp"
#include "commands/LogisticRegressionHyperparams.hpp"
#include "commands/Preprocessor.hpp"
#include "commands/XGBoostHyperparams.hpp"
#include "fastprop/Hyperparameters.hpp"

#include <rfl/Field.hpp>
#include <rfl/Flatten.hpp>
#include <rfl/Literal.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/TaggedUnion.hpp>
#include <rfl/define_tagged_union.hpp>
#include <rfl/define_variant.hpp>
#include <rfl/json/Reader.hpp>

#include <variant>
#include <vector>

namespace commands {

struct Fingerprint {
  // -----------------------------
  // Data frame fingerprints

  /// For retrieving ordinary data frames that were neither created by a view
  /// nor a pipeline.
  struct OrdinaryDataFrame {
    rfl::Field<"name_", std::string> name;
    rfl::Field<"last_change_", std::string> last_change;
  };

  /// For retrieving data frames that are the results of entire pipelines (and
  /// thus contain features).
  struct PipelineBuildHistory {
    rfl::Field<"dependencies_", std::vector<Fingerprint>> dependencies;
    rfl::Field<"df_fingerprints_", std::vector<commands::Fingerprint>>
        df_fingerprints;
  };

  using DataFrameFingerprint =
      std::variant<typename DataFrameOrView::ViewOp, OrdinaryDataFrame,
                   rfl::Ref<const DataModel>, PipelineBuildHistory>;

  // -----------------------------
  // Preprocessors

  /// The fingerprint for a CategoryTrimmer.
  struct CategoryTrimmerFingerprint {
    rfl::Field<"dependencies_", std::vector<Fingerprint>> dependencies;
    rfl::Flatten<typename Preprocessor::CategoryTrimmerOp> op;
  };

  /// The fingerprint for an EmailDomain preprocessor.
  struct EMailDomainFingerprint {
    rfl::Field<"dependencies_", std::vector<Fingerprint>> dependencies;
    rfl::Flatten<typename Preprocessor::EMailDomainOp> op;
  };

  /// The fingerprint for an Imputation preprocessor.
  struct ImputationFingerprint {
    rfl::Field<"dependencies_", std::vector<Fingerprint>> dependencies;
    rfl::Flatten<typename Preprocessor::ImputationOp> op;
  };

  /// The fingerprint for a Seasonal preprocessor.
  struct SeasonalFingerprint {
    rfl::Field<"dependencies_", std::vector<Fingerprint>> dependencies;
    rfl::Flatten<typename Preprocessor::SeasonalOp> op;
  };

  /// The fingerprint for a Substring preprocessor.
  struct SubstringFingerprint {
    rfl::Field<"dependencies_", std::vector<Fingerprint>> dependencies;
    rfl::Flatten<typename Preprocessor::SubstringOp> op;
  };

  /// The fingerprint for a TextFieldSplitter preprocessor.
  struct TextFieldSplitterFingerprint {
    rfl::Field<"dependencies_", std::vector<Fingerprint>> dependencies;
    rfl::Flatten<typename Preprocessor::TextFieldSplitterOp> op;
  };

  using PreprocessorFingerprint =
      std::variant<CategoryTrimmerFingerprint, EMailDomainFingerprint,
                   ImputationFingerprint, SeasonalFingerprint,
                   SubstringFingerprint, TextFieldSplitterFingerprint>;

  // -----------------------------

  /// FeatureLearner fingerprints also require this.
  struct OtherFLRequirements {
    rfl::Field<"peripheral_", rfl::Ref<const std::vector<std::string>>>
        peripheral;
    rfl::Field<"placeholder_", rfl::Ref<const helpers::Placeholder>>
        placeholder;
    rfl::Field<"target_num_", Int> target_num;
  };

  /// The fingerprint for a FastProp feature learner.
  struct FastPropFingerprint {
    rfl::Flatten<fastprop::Hyperparameters> hyperparams;
    rfl::Field<"dependencies_", std::vector<Fingerprint>> dependencies;
    rfl::Flatten<OtherFLRequirements> other;
  };

  using FeatureLearnerFingerprint = std::variant<FastPropFingerprint>;

  // -----------------------------
  // Predictors

  /// There are different predictors for every target, so the target number must
  /// also be supported as a dependency.
  struct TargetNumber {
    rfl::Field<"target_num_", size_t> target_num;
  };

  /// The predictors also require information about the exact columns inserted
  /// into them.
  struct OtherPredRequirements {
    rfl::Field<"autofeatures_", std::vector<std::vector<size_t>>> autofeatures;
    rfl::Field<"categorical_colnames_", std::vector<std::string>>
        categorical_colnames;
    rfl::Field<"numerical_colnames_", std::vector<std::string>>
        numerical_colnames;
  };

  /// The fingerprint for a LinearRegression.
  struct LinearRegressionFingerprint {
    rfl::Flatten<LinearRegressionHyperparams> hyperparams;
    rfl::Field<"dependencies_", std::vector<Fingerprint>> dependencies;
    rfl::Flatten<OtherPredRequirements> other;
  };

  /// The fingerprint for a LinearRegression.
  struct LogisticRegressionFingerprint {
    rfl::Flatten<LogisticRegressionHyperparams> hyperparams;
    rfl::Field<"dependencies_", std::vector<Fingerprint>> dependencies;
    rfl::Flatten<OtherPredRequirements> other;
  };

  /// The fingerprint for an XGBoostPredictor.
  struct XGBoostFingerprint {
    rfl::Flatten<XGBoostHyperparams> hyperparams;
    rfl::Field<"dependencies_", std::vector<Fingerprint>> dependencies;
    rfl::Flatten<OtherPredRequirements> other;
  };

  using PredictorFingerprint =
      std::variant<LinearRegressionFingerprint, LogisticRegressionFingerprint,
                   XGBoostFingerprint, TargetNumber>;

  // -----------------------------

  using ReflectionType =
      rfl::define_variant_t<DataFrameFingerprint, PreprocessorFingerprint,
                            FeatureLearnerFingerprint, PredictorFingerprint>;

  // -----------------------------

  explicit Fingerprint(const ReflectionType& _val);

  static Fingerprint from_json(const std::string& _json_str);

  using InputVarType = typename rfl::json::Reader::InputVarType;

  static Fingerprint from_json_obj(const InputVarType& _json_obj);

  std::string to_json() const;

  ~Fingerprint() = default;

  ReflectionType val_;
};

}  // namespace commands

#endif  // COMMANDS_FINGERPRINT_HPP_
