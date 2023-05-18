// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef JSON_PARSER_HPP_
#define JSON_PARSER_HPP_

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <simdjson.h>

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

struct PocoJSONParser {
  using InputArrayType = Poco::JSON::Array::Ptr;
  using InputObjectType = Poco::JSON::Object::Ptr;
  using InputVarType = Poco::Dynamic::Var;

  using OutputArrayType = Poco::JSON::Array::Ptr;
  using OutputObjectType = Poco::JSON::Object::Ptr;
  using OutputVarType = Poco::Dynamic::Var;

  static void add(const OutputVarType _var, OutputArrayType* _arr) noexcept {
    (*_arr)->add(_var);
  }

  static OutputVarType empty_var() { return Poco::Dynamic::Var(); }

  static fct::Result<InputVarType> get(const size_t _i,
                                       const InputArrayType* _arr) noexcept {
    try {
      assert_true(_arr);
      assert_true(*_arr);
      return (*_arr)->get(_i);
    } catch (std::exception& e) {
      return fct::Error("Could not get element from array: " +
                        std::string(e.what()));
    }
  }

  static fct::Result<size_t> get_array_size(InputArrayType* _arr) noexcept {
    return (*_arr)->size();
  }

  static fct::Result<InputVarType> get_field(const std::string& _name,
                                             InputObjectType* _obj) noexcept {
    try {
      assert_true(_obj);
      assert_true(*_obj);
      return (*_obj)->get(_name);
    } catch (std::exception& e) {
      return fct::Error("Could not get field from object: " +
                        std::string(e.what()));
    }
  }

  static bool has_key(const std::string& _key, InputObjectType* _obj) noexcept {
    return (*_obj)->has(_key);
  }

  static OutputArrayType new_array() noexcept {
    return Poco::JSON::Array::Ptr(new Poco::JSON::Array());
  }

  static OutputObjectType new_object() noexcept {
    return Poco::JSON::Object::Ptr(new Poco::JSON::Object());
  }

  static bool is_empty(InputVarType* _var) noexcept { return _var->isEmpty(); }

  static void set_field(const std::string& _name, const OutputVarType& _var,
                        OutputObjectType* _obj) noexcept {
    (*_obj)->set(_name, _var);
  }

  template <class T>
  static fct::Result<T> to_basic_type(InputVarType* _var) noexcept {
    try {
      assert_true(_var);
      return _var->convert<std::decay_t<T>>();
    } catch (std::exception& e) {
      return fct::Error("Could not cast to expected type: " +
                        std::string(e.what()));
    }
  }

  static fct::Result<InputArrayType> to_array(InputVarType* _var) noexcept {
    try {
      assert_true(_var);
      auto arr = _var->extract<InputArrayType>();
      if (!arr) {
        return fct::Error("Pointer was empty!");
      }
      return arr;
    } catch (std::exception& _exp) {
    }

    try {
      assert_true(_var);
      const auto arr = _var->extract<Poco::JSON::Array>();
      return Poco::JSON::Array::Ptr(new Poco::JSON::Array(arr));
    } catch (std::exception& e) {
      return fct::Error("Could not cast to array.");
    }
  }

  static std::map<std::string, InputVarType> to_map(
      InputObjectType* _obj) noexcept {
    const auto names = (*_obj)->getNames();
    const auto to_pair =
        [_obj](const auto& _name) -> std::pair<std::string, InputVarType> {
      return std::make_pair(_name, (*_obj)->get(_name));
    };
    return fct::collect::map<std::string, InputVarType>(
        names | VIEWS::transform(to_pair));
  }

  static fct::Result<InputObjectType> to_object(InputVarType* _var) noexcept {
    try {
      assert_true(_var);
      auto obj = _var->extract<InputObjectType>();
      if (!obj) {
        return fct::Error("Pointer was empty!");
      }
      return obj;
    } catch (std::exception& _exp) {
    }

    try {
      assert_true(_var);
      const auto obj = _var->extract<Poco::JSON::Object>();
      return Poco::JSON::Object::Ptr(new Poco::JSON::Object(obj));
    } catch (std::exception& e) {
      return fct::Error("Could not cast to object.");
    }
  }

  static std::vector<InputVarType> to_vec(InputArrayType* _arr) noexcept {
    const auto iota = fct::iota<size_t>(0, (*_arr)->size());
    const auto get = [_arr](const size_t _i) { return (*_arr)->get(_i); };
    return fct::collect::vector(iota | VIEWS::transform(get));
  }
};

struct SimdJSONParser {
  using ArrayType = simdjson::ondemand::array;
  using ObjectType = simdjson::ondemand::object;
  using VarType = simdjson::ondemand::value;

  // TODO
  /*  static void add(const VarType _var, ArrayType* _arr) {
      assert_true(_arr);
      assert_true(*_arr);
      (*_arr)->add(_var);
    }*/

  // TODO
  // static VarType empty_var() { return Poco::Dynamic::Var(); }

  static fct::Result<VarType> get(const size_t _i,
                                  simdjson::ondemand::array* _arr) {
    size_t i = 0;
    for (auto val : *_arr) {
      if (i == _i) {
        return val.value();
      }
      ++i;
    }
    return fct::Error("Index " + std::to_string(_i) +
                      " out of range. Length was " + std::to_string(i) + ".");
  }

  static size_t get_array_size(ArrayType* _arr) {
    return _arr->count_elements();
  }

  static fct::Result<VarType> get_field(const std::string& _name,
                                        ObjectType* _obj) {
    auto res = (*_obj)[_name];
    VarType v;
    const auto error = res.get(v);
    if (error) {
      return fct::Error("Object contains no field named '" + _name + "'.");
    }
    return v;
  }

  static std::vector<std::string> get_names(ObjectType* _obj) {
    std::vector<std::string> names;
    for (auto field : *_obj) {
      names.push_back(std::string(std::string_view(field.unescaped_key())));
    }
    return names;
  }

  static bool has_key(const std::string& _key, ObjectType* _obj) {
    for (auto field : *_obj) {
      if (_key == std::string_view(field.unescaped_key())) {
        return true;
      }
    }
    return false;
  }

  static ArrayType new_array() { return simdjson::ondemand::array(); }

  static ObjectType new_object() { return simdjson::ondemand::object(); }

  //  static bool is_empty(const VarType& _var) { return _var.isEmpty(); }

  /* static void set_field(const std::string& _name, const VarType& _var,
                         ObjectType* _obj) {
     assert_true(_obj);
     assert_true(*_obj);
     (*_obj)->set(_name, _var);
   }*/

  template <class T>
  static fct::Result<T> to_basic_type(const VarType& _var) {
    try {
      T var = _var;
      return var;
    } catch (std::exception& e) {
      return fct::Error("Could not cast to expected type.");
    }
  }

  static ArrayType to_array(VarType* _var) { return _var->get_array(); }

  static ObjectType to_object(VarType* _var) { return _var->get_object(); }
};

using JSONParser = PocoJSONParser;

template <class T>
using Parser = parsing::Parser<JSONParser, T>;
};  // namespace json

#endif  // JSON_PARSER_HPP_
