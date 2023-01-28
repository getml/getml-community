// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_PREPROCESSORPARSER_HPP_
#define ENGINE_PREPROCESSORS_PREPROCESSORPARSER_HPP_

#include <Poco/JSON/Object.h>

#include <vector>

#include "engine/preprocessors/Preprocessor.hpp"
#include "fct/Ref.hpp"

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

