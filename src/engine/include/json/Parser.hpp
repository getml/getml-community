// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef JSON_PARSER_HPP_
#define JSON_PARSER_HPP_

#include <yyjson.h>

#include <exception>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "fct/Result.hpp"
#include "fct/collect.hpp"
#include "fct/ranges.hpp"
#include "json/has_from_json_obj.hpp"
#include "parsing/Parser.hpp"

namespace json {

struct YYJSONInputArray {
  yyjson_val* val_;
};

struct YYJSONInputObject {
  yyjson_val* val_;
};

struct YYJSONInputVar {
  yyjson_val* val_;
};

struct YYJSONOutputArray {
  yyjson_mut_doc* doc_;
  yyjson_mut_val* val_;
};

struct YYJSONOutputObject {
  yyjson_mut_doc* doc_;
  yyjson_mut_val* val_;
};

struct YYJSONOutputVar {
  yyjson_mut_doc* doc_;
  yyjson_mut_val* val_;
};

struct YYJSONOutputContext {
  yyjson_mut_doc* doc_;
};

struct YYJSONParser {
  using InputArrayType = YYJSONInputArray;
  using InputObjectType = YYJSONInputObject;
  using InputVarType = YYJSONInputVar;

  using OutputArrayType = YYJSONOutputArray;
  using OutputObjectType = YYJSONOutputObject;
  using OutputVarType = YYJSONOutputVar;

  using OutputContextType = YYJSONOutputContext;

  static void add(const OutputVarType _var, OutputArrayType* _arr) noexcept {
    yyjson_mut_arr_add_val(_arr->val_, _var.val_);
  }

  static OutputVarType empty_var() noexcept { return Poco::Dynamic::Var(); }

  static fct::Result<InputVarType> get_field(const std::string& _name,
                                             InputObjectType* _obj) noexcept {
    const auto var = InputVarType(yyjson_obj_get(_obj->val_, _name.c_str()));
    if (!val.val_) {
      return fct::Error("Object contains no field named '" + _name + "'.");
    }
    return var;
  }

  static OutputArrayType new_array() noexcept {
    return Poco::JSON::Array::Ptr(new Poco::JSON::Array());
  }

  static OutputObjectType new_object() noexcept {
    return Poco::JSON::Object::Ptr(new Poco::JSON::Object());
  }

  static bool is_empty(InputVarType* _var) noexcept {
    return !_var->val_ || yyjson_is_null(_var->val_);
  }

  static bool is_empty(OutputVarType* _var) noexcept {
    return yyjson_mut_is_null(_val->val_);
  }

  static void set_field(const std::string& _name, const OutputVarType& _var,
                        OutputObjectType* _obj) noexcept {
    yyjson_mut_obj_add_val(_obj->doc_, _obj->val_, _name.c_str(), _var.val_);
  }

  template <class T>
  static fct::Result<T> to_basic_type(InputVarType* _var) noexcept {
    if constexpr (std::is_same<std::decay_t<T>, std::string>()) {
      const auto r = yyjson_get_str(_var->val_);
      if (r == NULL) {
        return fct::Error("Could not cast to string.");
      }
      return std::string(r);
    } else if constexpr (std::is_same<std::decay_t<T>, bool>()) {
      if (!yyjson_is_bool(_var->val_)) {
        return fct::Error("Could not cast to boolean.");
      }
      return yyjson_get_bool(_var->val_);
    } else if constexpr (std::is_floating_point<std::decay_t<T>>()) {
      if (!yyjson_is_num(_var->val_)) {
        return fct::Error("Could not cast to double.");
      }
      return static_cast<T>(yyjson_get_real(_var->val_));
    } else {
      if (!yyjson_is_int(_var->val_)) {
        return fct::Error("Could not cast to int.");
      }
      return static_cast<T>(yyjson_get_int(_var->val_));
    }
  }

  static fct::Result<InputArrayType> to_array(InputVarType* _var) noexcept {
    if (!yyjson_is_arr(_var->val_)) {
      return fct::Error("Could not cast to array!");
    }
    return InputArrayType(_var->val_);
  }

  static std::map<std::string, InputVarType> to_map(
      InputObjectType* _obj) noexcept {
    std::map<std::string, InputVarType> m;
    yyjson_obj_iter iter;
    yyjson_obj_iter_init(_obj->val_, &iter);
    yyjson_val* key;
    while ((key = yyjson_obj_iter_next(&iter))) {
      m[yyjson_get_str(key)] = yyjson_obj_iter_get_val(key);
    }
    return m;
  }

  static fct::Result<InputObjectType> to_object(InputVarType* _var) noexcept {
    if (!yyjson_is_obj(_var->val_)) {
      return fct::Error("Could not cast to object!");
    }
    return InputObjectType(_var->val_);
  }

  static std::vector<InputVarType> to_vec(InputArrayType* _arr) noexcept {
    std::vector<InputVarType> vec;
    yyjson_val* val;
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(_arr->val_, &iter);
    while ((val = yyjson_arr_iter_next(&iter))) {
      vec.push_back(val);
    }
    return vec;
  }
};

using JSONParser = YYJSONParser;

template <class T>
using Parser = parsing::Parser<JSONParser, T>;
};  // namespace json

#endif  // JSON_PARSER_HPP_
