// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef JSON_FROM_JSON_HPP_
#define JSON_FROM_JSON_HPP_

#include <yyjson.h>

#include <string>

#include "json/Parser.hpp"

namespace json {

using InputObjectType = typename JSONReader::InputObjectType;
using InputVarType = typename JSONReader::InputVarType;

/// Parses an object from JSON using reflection.
template <class T>
T from_json(const std::string& _json_str) {
  yyjson_doc* doc = yyjson_read(_json_str.c_str(), _json_str.size(), 0);
  InputVarType root = InputVarType(yyjson_doc_get_root(doc));
  const auto r = JSONReader();
  const auto result = Parser<T>::from_json(r, &root);
  yyjson_doc_free(doc);
  return result.value();
}

/// Parses an object from JSON using reflection.
template <class T>
T from_json(const InputVarType& _var) {
  InputVarType var = _var;
  const auto r = JSONReader();
  return Parser<T>::from_json(r, &var).value();
}

/// Parses an object from JSON using reflection.
template <class T>
T from_json(const InputObjectType& _obj) {
  return from_json<T>(InputVarType(_obj.val_));
}

}  // namespace json

#endif  // JSON_FROM_JSON_HPP_
