// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_PREDICTORFINGERPRINT_HPP_
#define COMMANDS_PREDICTORFINGERPRINT_HPP_

#include <cstddef>
#include <string>
#include <vector>

#include "commands/DataFrameFingerprint.hpp"
#include "commands/FeatureLearnerFingerprint.hpp"
#include "commands/LinearRegressionHyperparams.hpp"
#include "commands/LogisticRegressionHyperparams.hpp"
#include "commands/PreprocessorFingerprint.hpp"
#include "commands/XGBoostHyperparams.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "fct/TaggedUnion.hpp"
#include "fct/define_named_tuple.hpp"
#include "helpers/Placeholder.hpp"

namespace commands {

struct PredictorFingerprint {
  /// Because predictors can also be feature selectors, the recursive definition
  /// is necessary.
  using DependencyType =
      std::variant<DataFrameFingerprint, PreprocessorFingerprint,
                   FeatureLearnerFingerprint, PredictorFingerprint>;

  /// This needs to be added to every fingerprint.
  using Dependencies =
      fct::NamedTuple<fct::Field<"dependencies_", std::vector<DependencyType>>>;

  /// The predictors also require information about the exact columns inserted
  /// into them.
  using OtherRequirements = fct::NamedTuple<
      fct::Field<"autofeatures_", std::vector<std::vector<size_t>>>,
      fct::Field<"categorical_colnames_", std::vector<std::string>>,
      fct::Field<"numerical_colnames_", std::vector<std::string>>>;

  /// The fingerprint for a LinearRegression.
  using LinearRegressionFingerprint = fct::define_named_tuple_t<
      typename LinearRegressionHyperparams::NamedTupleType, Dependencies,
      OtherRequirements>;

  /// The fingerprint for a LinearRegression.
  using LogisticRegressionFingerprint = fct::define_named_tuple_t<
      typename LogisticRegressionHyperparams::NamedTupleType, Dependencies,
      OtherRequirements>;

  /// The fingerprint for an XGBoostPredictor.
  using XGBoostFingerprint =
      fct::define_named_tuple_t<typename XGBoostHyperparams::NamedTupleType,
                                Dependencies, OtherRequirements>;

  using NamedTupleType =
      fct::TaggedUnion<"type_", LinearRegressionFingerprint,
                       LogisticRegressionFingerprint, XGBoostFingerprint>;

  PredictorFingerprint(const NamedTupleType& _val) : val_(_val) {}

  PredictorFingerprint(const typename NamedTupleType::VariantType& _variant)
      : val_(_variant) {}

  ~PredictorFingerprint() = default;

  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_PREDICTORFINGERPRINT_HPP_
