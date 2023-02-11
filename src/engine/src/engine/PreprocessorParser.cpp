// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/preprocessors/CategoryTrimmer.hpp"
#include "engine/preprocessors/EMailDomain.hpp"
#include "engine/preprocessors/Imputation.hpp"
#include "engine/preprocessors/Seasonal.hpp"
#include "engine/preprocessors/Substring.hpp"
#include "engine/preprocessors/TextFieldSplitter.hpp"
#include "engine/preprocessors/preprocessors.hpp"
#include "fct/visit.hpp"
#include "json/json.hpp"

namespace engine {
namespace preprocessors {

fct::Ref<Preprocessor> PreprocessorParser::parse(
    const PreprocessorHyperparams& _cmd,
    const std::vector<DependencyType>& _dependencies) {
  const auto handle =
      [&_dependencies](const auto& _hyperparams) -> fct::Ref<Preprocessor> {
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
      return fct::Ref<CategoryTrimmer>::make(_hyperparams, _dependencies);
    }

    if constexpr (std::is_same<Type, EMailDomainOp>()) {
      return fct::Ref<EMailDomain>::make(_hyperparams, _dependencies);
    }

    if constexpr (std::is_same<Type, ImputationOp>()) {
      return fct::Ref<Imputation>::make(_hyperparams, _dependencies);
    }

    if constexpr (std::is_same<Type, SeasonalOp>()) {
      return fct::Ref<Seasonal>::make(_hyperparams, _dependencies);
    }

    if constexpr (std::is_same<Type, SubstringOp>()) {
      return fct::Ref<Substring>::make(_hyperparams, _dependencies);
    }

    if constexpr (std::is_same<Type, TextFieldSplitterOp>()) {
      return fct::Ref<TextFieldSplitter>::make(_hyperparams, _dependencies);
    }
  };

  return fct::visit(handle, _cmd);
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
