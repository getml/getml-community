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
  yyjson_doc* doc = yyjson_read(_json_str.c_str(), _json_str.size(), 0);
  yyjson_val* root = yyjson_doc_get_root(doc);
  const auto result = Parser<T>::from_json(&root);
  yyjson_doc_free(doc);
  return result.value();
}

/// Parses an object from JSON using reflection.
template <class T>
T from_json(const InputVarType& _var) {
  InputVarType var = _var;
  return Parser<T>::from_json(&var).value();
}

}  // namespace json

#endif  // JSON_FROM_JSON_HPP_
