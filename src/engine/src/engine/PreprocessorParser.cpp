// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include <stdexcept>

#include "engine/preprocessors/CategoryTrimmer.hpp"
#include "engine/preprocessors/EMailDomain.hpp"
#include "engine/preprocessors/Imputation.hpp"
#include "engine/preprocessors/Seasonal.hpp"
#include "engine/preprocessors/Substring.hpp"
#include "engine/preprocessors/TextFieldSplitter.hpp"
#include "engine/preprocessors/preprocessors.hpp"
#include "rfl/json.hpp"
#include "rfl/visit.hpp"

namespace engine {
namespace preprocessors {

rfl::Ref<Preprocessor> PreprocessorParser::parse(
    const PreprocessorHyperparams& _cmd,
    const std::vector<commands::Fingerprint>& _dependencies) {
  const auto handle =
      [&_dependencies](const auto& _hyperparams) -> rfl::Ref<Preprocessor> {
    using CategoryTrimmerOp =
        typename commands::Preprocessor::CategoryTrimmerOp;
    using EMailDomainOp = typename commands::Preprocessor::EMailDomainOp;
    using ImputationOp = typename commands::Preprocessor::ImputationOp;
    using SeasonalOp = typename commands::Preprocessor::SeasonalOp;
    using SubstringOp = typename commands::Preprocessor::SubstringOp;
    using TextFieldSplitterOp =
        typename commands::Preprocessor::TextFieldSplitterOp;

    using Type = std::decay_t<decltype(_hyperparams)>;

    if constexpr (std::is_same<Type, CategoryTrimmerOp>()) {
      return rfl::Ref<CategoryTrimmer>::make(_hyperparams, _dependencies);
    }

    else if constexpr (std::is_same<Type, EMailDomainOp>()) {
      return rfl::Ref<EMailDomain>::make(_hyperparams, _dependencies);
    }

    else if constexpr (std::is_same<Type, ImputationOp>()) {
      return rfl::Ref<Imputation>::make(_hyperparams, _dependencies);
    }

    else if constexpr (std::is_same<Type, SeasonalOp>()) {
      return rfl::Ref<Seasonal>::make(_hyperparams, _dependencies);
    }

    else if constexpr (std::is_same<Type, SubstringOp>()) {
      return rfl::Ref<Substring>::make(_hyperparams, _dependencies);
    }

    else if constexpr (std::is_same<Type, TextFieldSplitterOp>()) {
      return rfl::Ref<TextFieldSplitter>::make(_hyperparams, _dependencies);
    }

    else {
      throw std::runtime_error(
          "The " + _hyperparams.name() +
          " preprocessor is not supported in the community edition. Please "
          "upgrade to getML enterprise to use this. An overview of what is "
          "supported in the community edition can be found in the official "
          "getML documentation.");
    }
  };

  return rfl::visit(handle, _cmd);
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
