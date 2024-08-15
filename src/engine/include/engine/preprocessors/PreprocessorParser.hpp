// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_PREPROCESSORPARSER_HPP_
#define ENGINE_PREPROCESSORS_PREPROCESSORPARSER_HPP_

#include <rfl/Ref.hpp>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "commands/Preprocessor.hpp"
#include "engine/preprocessors/Preprocessor.hpp"

namespace engine {
namespace preprocessors {

struct PreprocessorParser {
  using PreprocessorHyperparams =
      typename commands::Preprocessor::ReflectionType;

  /// Returns the correct preprocessor to use based on the JSON object.
  static rfl::Ref<Preprocessor> parse(
      const PreprocessorHyperparams& _cmd,
      const std::vector<commands::Fingerprint>& _dependencies);
};

}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_PREPROCESSORPARSER_HPP_
