#include "engine/preprocessors/preprocessors.hpp"

// ----------------------------------------------------

#include "engine/preprocessors/EMailDomain.hpp"
#include "engine/preprocessors/Imputation.hpp"
#include "engine/preprocessors/Mapping.hpp"
#include "engine/preprocessors/Seasonal.hpp"
#include "engine/preprocessors/Substring.hpp"
#include "engine/preprocessors/TextFieldSplitter.hpp"

// ----------------------------------------------------

namespace engine {
namespace preprocessors {

fct::Ref<Preprocessor> PreprocessorParser::parse(
    const Poco::JSON::Object& _obj,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies) {
  const auto type = jsonutils::JSON::get_value<std::string>(_obj, "type_");

  if (type == Preprocessor::EMAILDOMAIN) {
    return fct::Ref<EMailDomain>::make(_obj, _dependencies);
  }

  if (type == Preprocessor::IMPUTATION) {
    return fct::Ref<Imputation>::make(_obj, _dependencies);
  }

  if (type == Preprocessor::MAPPING) {
    return fct::Ref<Mapping>::make(_obj, _dependencies);
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

  throw std::runtime_error("Preprocessor of type '" + type + "' not known!");
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
