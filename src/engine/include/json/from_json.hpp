// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef JSON_FROM_JSON_HPP_
#define JSON_FROM_JSON_HPP_

#include <string>

#include "json/Parser.hpp"

namespace json {

using InputObjectType = typename JSONParser::InputObjectType;
using InputVarType = typename JSONParser::InputVarType;

/// Parses an object from JSON using reflection.
template <class T>
T from_json(const std::string& _json_str) {
  InputVarType json_obj = json::JSONParser::from_string(_json_str);
  return Parser<T>::from_json(&json_obj).value();
}

/// Parses an object from JSON using reflection.
template <class T>
T from_json(const InputObjectType& _json_obj) {
  InputVarType json_obj = _json_obj;
  return Parser<T>::from_json(&json_obj).value();
}

/// Parses an object from JSON using reflection.
template <class T>
T from_json(const InputVarType& _var) {
  InputVarType var = _var;
  return Parser<T>::from_json(&var).value();
}

}  // namespace json

#endif  // JSON_FROM_JSON_HPP_
