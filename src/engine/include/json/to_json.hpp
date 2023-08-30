// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef JSON_TO_JSON_HPP_
#define JSON_TO_JSON_HPP_

#include <yyjson.h>

#include <sstream>
#include <string>

#include "json/Parser.hpp"

namespace json {

/// Parses an object to JSON.
template <class T>
std::string to_json(const T& _obj) {
  auto w = Writer(yyjson_mut_doc_new(NULL));
  const auto json_obj = Parser<T>::write(w, _obj);
  yyjson_mut_doc_set_root(w.doc_, json_obj.val_);
  const char* json_c_str = yyjson_mut_write(w.doc_, 0, NULL);
  const auto json_str = std::string(json_c_str);
  free((void*)json_c_str);
  yyjson_mut_doc_free(w.doc_);
  return json_str;
}

}  // namespace json

#endif  // JSON_PARSER_HPP_
