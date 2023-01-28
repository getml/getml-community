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

namespace engine {
namespace preprocessors {

fct::Ref<Preprocessor> PreprocessorParser::parse(
    const Poco::JSON::Object& _obj,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies) {
  const auto type = jsonutils::JSON::get_value<std::string>(_obj, "type_");

  if (type == Preprocessor::CATEGORY_TRIMMER) {
    return fct::Ref<CategoryTrimmer>::make(_obj, _dependencies);
  }

  if (type == Preprocessor::EMAILDOMAIN) {
    return fct::Ref<EMailDomain>::make(_obj, _dependencies);
  }

  if (type == Preprocessor::IMPUTATION) {
    return fct::Ref<Imputation>::make(_obj, _dependencies);
  }

  if (type == Preprocessor::SEASONAL) {
    return fct::Ref<Seasonal>::make(_obj, _dependencies);
  }

  if (type == Preprocessor::SUBSTRING) {
    return fct::Ref<Substring>::make(_obj, _dependencies);
  }

  if (type == Preprocessor::TEXT_FIELD_SPLITTER) {
    return fct::Ref<TextFieldSplitter>::make(_obj, _dependencies);
  }

  if (type == Preprocessor::MAPPING) {
    throw std::runtime_error("Preprocessor of type '" + type +
                             "' is not supported in the community edition!");
  }

  throw std::runtime_error("Preprocessor of type '" + type + "' not known!");
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
