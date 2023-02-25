// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_PREPROCESSORPARSER_HPP_
#define ENGINE_PREPROCESSORS_PREPROCESSORPARSER_HPP_

#include <vector>

#include "commands/Preprocessor.hpp"
#include "commands/PreprocessorFingerprint.hpp"
#include "engine/preprocessors/Preprocessor.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace preprocessors {

struct PreprocessorParser {
  using DependencyType =
      typename commands::PreprocessorFingerprint::DependencyType;

  using PreprocessorHyperparams =
      typename commands::Preprocessor::NamedTupleType;

  /// Returns the correct preprocessor to use based on the JSON object.
  static fct::Ref<Preprocessor> parse(
      const PreprocessorHyperparams& _cmd,
      const std::vector<DependencyType>& _dependencies);
};

}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_PREPROCESSORPARSER_HPP_

