// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_FEATURELEARNERFINGERPRINT_HPP_
#define ENGINE_COMMANDS_FEATURELEARNERFINGERPRINT_HPP_

#include <vector>

#include "engine/commands/DataFrameFingerprint.hpp"
#include "engine/commands/FeatureLearner.hpp"
#include "engine/commands/PreprocessorFingerprint.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "fct/TaggedUnion.hpp"
#include "fct/define_named_tuple.hpp"
#include "helpers/Placeholder.hpp"

namespace engine {
namespace commands {

struct FeatureLearnerFingerprint {
  /// If there are no preprocessors, the dependencies are DataFrameFingerprints.
  using DependencyType =
      std::variant<DataFrameFingerprint, PreprocessorFingerprint>;

  /// This needs to be added to every fingerprint.
  using Dependencies = fct::NamedTuple<
      fct::Field<"dependencies_", fct::Ref<const std::vector<DependencyType>>>>;

  /// FeatureLearner fingerprints also require this.
  using OtherRequirements = fct::NamedTuple<
      fct::Field<"peripheral_", fct::Ref<const std::vector<std::string>>>,
      fct::Field<"placeholder_", fct::Ref<const helpers::Placeholder>>,
      fct::Field<"target_num_", Int>>;

  /// The fingerprint for a FastProp feature learner.
  using FastPropFingerprint = fct::define_named_tuple_t<
      typename fastprop::Hyperparameters::NamedTupleType, Dependencies,
      OtherRequirements>;

  using NamedTupleType = fct::TaggedUnion<"type_", FastPropFingerprint>;

  FeatureLearnerFingerprint(const NamedTupleType& _val) : val_(_val) {}

  FeatureLearnerFingerprint(
      const typename NamedTupleType::VariantType& _variant)
      : val_(_variant) {}

  ~FeatureLearnerFingerprint() = default;

  NamedTupleType val_;
};

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_FEATURELEARNERFINGERPRINT_HPP_
