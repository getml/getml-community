// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef JSON_FROM_JSON_HPP_
#define JSON_FROM_JSON_HPP_

#include <sstream>
#include <string>

#include "Poco/JSON/Parser.h"
#include "json/Parser.hpp"

namespace json {

/// Parses an object from JSON using reflection.
template <class T>
T from_json(const std::string& _json_str) {
  Poco::JSON::Parser parser;
  const auto json_obj = parser.parse(_json_str);
  return Parser<T>::from_json(json_obj);
}

/// Parses an object from JSON using reflection.
template <class T>
T from_json(const Poco::JSON::Object& _json_obj) {
  return Parser<T>::from_json(_json_obj);
}

/// Parses an object from JSON using reflection.
template <class T>
T from_json(const Poco::Dynamic::Var& _json_obj) {
  return Parser<T>::from_json(_json_obj);
}

}  // namespace json

#endif  // JSON_FROM_JSON_HPP_
