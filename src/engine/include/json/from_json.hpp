// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef JSON_FROM_JSON_HPP_
#define JSON_FROM_JSON_HPP_

#include <string>

#include "Poco/JSON/Parser.h"
#include "json/Parser.hpp"

namespace json {

/// Parses an object from JSON using reflection.
template <class T>
T from_json(const std::string& _json_str) {
  Poco::JSON::Parser parser;
  auto json_obj = parser.parse(_json_str);
  return Parser<T>::from_json(&json_obj).value();
}

/// Parses an object from JSON using reflection.
template <class T>
T from_json(const Poco::JSON::Object::Ptr& _json_obj) {
  Poco::Dynamic::Var json_obj = _json_obj;
  return Parser<T>::from_json(&json_obj).value();
}

/// Parses an object from JSON using reflection.
template <class T>
T from_json(const Poco::Dynamic::Var& _var) {
  Poco::Dynamic::Var var = _var;
  return Parser<T>::from_json(&var).value();
}

}  // namespace json

#endif  // JSON_FROM_JSON_HPP_
