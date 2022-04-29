#ifndef ENGINE_PREPROCESSORS_PREPROCESSORPARSER_HPP_
#define ENGINE_PREPROCESSORS_PREPROCESSORPARSER_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <vector>

// ----------------------------------------------------------------------------

#include "fct/Ref.hpp"

// ----------------------------------------------------------------------------

#include "engine/preprocessors/Preprocessor.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace preprocessors {

struct PreprocessorParser {
  /// Returns the correct preprocessor to use based on the JSON object.
  static fct::Ref<Preprocessor> parse(
      const Poco::JSON::Object& _obj,
      const std::vector<Poco::JSON::Object::Ptr>& _dependencies);
};

}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_PREPROCESSORPARSER_HPP_

