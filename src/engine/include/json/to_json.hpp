// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef JSON_TO_JSON_HPP_
#define JSON_TO_JSON_HPP_

#include <sstream>
#include <string>

#include "Poco/JSON/Stringifier.h"
#include "json/Parser.hpp"

namespace json {

/// Parses an object to JSON.
template <class T>
std::string to_json(const T& _obj) {
  const auto json_obj = Parser<T>::to_json(_obj);
  std::stringstream stream;
  Poco::JSON::Stringifier::stringify(json_obj, stream);
  return stream.str();
}

}  // namespace json

#endif  // JSON_PARSER_HPP_
