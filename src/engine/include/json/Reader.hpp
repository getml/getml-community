// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef JSON_READER_HPP_
#define JSON_READER_HPP_

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

namespace json {

struct YYJSONOutputArray {
  yyjson_mut_val* val_;
};

struct YYJSONOutputObject {
  yyjson_mut_val* val_;
};

struct YYJSONOutputVar {
  YYJSONOutputVar(yyjson_mut_val* _val) : val_(_val) {}

  YYJSONOutputVar(YYJSONOutputArray _arr) : val_(_arr.val_) {}

  YYJSONOutputVar(YYJSONOutputObject _obj) : val_(_obj.val_) {}

  yyjson_mut_val* val_;
};

struct Reader {
  struct YYJSONInputArray {
    yyjson_val* val_;
  };

  struct YYJSONInputObject {
    yyjson_val* val_;
  };

  struct YYJSONInputVar {
    yyjson_val* val_;
  };

  using InputArrayType = YYJSONInputArray;
  using InputObjectType = YYJSONInputObject;
  using InputVarType = YYJSONInputVar;

  fct::Result<InputVarType> get_field(const std::string& _name,
                                      InputObjectType* _obj) const noexcept {
    const auto var = InputVarType(yyjson_obj_get(_obj->val_, _name.c_str()));
    if (!var.val_) {
      return fct::Error("Object contains no field named '" + _name + "'.");
    }
    return var;
  }

  bool is_empty(InputVarType* _var) const noexcept {
    return !_var->val_ || yyjson_is_null(_var->val_);
  }

  template <class T>
  fct::Result<T> to_basic_type(InputVarType* _var) const noexcept {
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

  fct::Result<InputArrayType> to_array(InputVarType* _var) const noexcept {
    if (!yyjson_is_arr(_var->val_)) {
      return fct::Error("Could not cast to array!");
    }
    return InputArrayType(_var->val_);
  }

  std::map<std::string, InputVarType> to_map(
      InputObjectType* _obj) const noexcept {
    std::map<std::string, InputVarType> m;
    yyjson_obj_iter iter;
    yyjson_obj_iter_init(_obj->val_, &iter);
    yyjson_val* key;
    while ((key = yyjson_obj_iter_next(&iter))) {
      const auto p = std::make_pair(yyjson_get_str(key),
                                    InputVarType(yyjson_obj_iter_get_val(key)));
      m.emplace(std::move(p));
    }
    return m;
  }

  fct::Result<InputObjectType> to_object(InputVarType* _var) const noexcept {
    if (!yyjson_is_obj(_var->val_)) {
      return fct::Error("Could not cast to object!");
    }
    return InputObjectType(_var->val_);
  }

  std::vector<InputVarType> to_vec(InputArrayType* _arr) const noexcept {
    std::vector<InputVarType> vec;
    yyjson_val* val;
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(_arr->val_, &iter);
    while ((val = yyjson_arr_iter_next(&iter))) {
      vec.push_back(InputVarType(val));
    }
    return vec;
  }
};

};  // namespace json

#endif  // JSON_PARSER_HPP_
