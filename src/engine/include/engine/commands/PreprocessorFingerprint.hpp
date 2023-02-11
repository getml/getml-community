// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_PREPROCESSORFINGERPRINT_HPP_
#define ENGINE_COMMANDS_PREPROCESSORFINGERPRINT_HPP_

#include "engine/commands/DataFrameFingerprint.hpp"
#include "engine/commands/Preprocessor.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/TaggedUnion.hpp"
#include "fct/define_named_tuple.hpp"

namespace engine {
namespace commands {

struct PreprocessorFingerprint {
  /// A preprocessor can depend on the data frames or other
  /// preprocessors.
  using DependencyType =
      std::variant<PreprocessorFingerprint, DataFrameFingerprint>;

  /// This needs to be added to every fingerprint.
  using Dependencies =
      fct::NamedTuple<fct::Field<"dependencies_", std::vector<DependencyType>>>;

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

  using NamedTupleType =
      fct::TaggedUnion<"type_", CategoryTrimmerFingerprint,
                       EMailDomainFingerprint, ImputationFingerprint,
                       SeasonalFingerprint, SubstringFingerprint,
                       TextFieldSplitterFingerprint>;

  PreprocessorFingerprint(const NamedTupleType& _val) : val_(_val) {}

  PreprocessorFingerprint(const typename NamedTupleType::VariantType& _variant)
      : val_(_variant) {}

  ~PreprocessorFingerprint() = default;

  NamedTupleType val_;
};

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_PREPROCESSORFINGERPRINT_HPP_
