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
#include <Poco/JSON/Parser.h>
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

  static InputVarType from_string(const std::string& _str) {
    Poco::JSON::Parser parser;
    return parser.parse(_str);
  }

  static OutputVarType empty_var() { return Poco::Dynamic::Var(); }

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
  using InputArrayType = simdjson::ondemand::array;
  using InputObjectType = simdjson::ondemand::object;
  using InputVarType = simdjson::ondemand::value;

  using OutputArrayType = Poco::JSON::Array::Ptr;
  using OutputObjectType = Poco::JSON::Object::Ptr;
  using OutputVarType = Poco::Dynamic::Var;

  static void add(const OutputVarType _var, OutputArrayType* _arr) noexcept {
    (*_arr)->add(_var);
  }

  static OutputVarType empty_var() noexcept { return Poco::Dynamic::Var(); }

  static InputVarType from_string(const std::string& _str) {
    simdjson::padded_string padded_str(_str);
    simdjson::ondemand::parser parser;
    return parser.iterate(padded_str);
  }

  static fct::Result<InputVarType> get_field(const std::string& _name,
                                             InputObjectType* _obj) noexcept {
    auto res = (*_obj)[_name];
    InputVarType v;
    const auto error = res.get(v);
    if (error) {
      return fct::Error("Object contains no field named '" + _name + "'.");
    }
    return v;
  }

  static OutputArrayType new_array() noexcept {
    return Poco::JSON::Array::Ptr(new Poco::JSON::Array());
  }

  static OutputObjectType new_object() noexcept {
    return Poco::JSON::Object::Ptr(new Poco::JSON::Object());
  }

  static bool is_empty(InputVarType* _var) noexcept { return _var->is_null(); }

  static bool is_empty(OutputVarType* _var) noexcept { return _var->isEmpty(); }

  static void set_field(const std::string& _name, const OutputVarType& _var,
                        OutputObjectType* _obj) noexcept {
    (*_obj)->set(_name, _var);
  }

  template <class T>
  static fct::Result<T> to_basic_type(InputVarType* _var) noexcept {
    if constexpr (std::is_same<std::decay_t<T>, std::string>()) {
      auto r = _var->get_string();
      if (r.error()) {
        return fct::Error("Could not cast to string.");
      }
      return std::string(r.value());
    } else if constexpr (std::is_same<std::decay_t<T>, bool>()) {
      auto r = _var->get_bool();
      if (r.error()) {
        return fct::Error("Could not cast to boolean.");
      }
      return r.value();
    } else if constexpr (std::is_floating_point<std::decay_t<T>>()) {
      auto r = _var->get_double();
      if (r.error()) {
        return fct::Error("Could not cast to double.");
      }
      return static_cast<T>(r.value());
    } else {
      auto r = _var->get_int64();
      if (r.error()) {
        return fct::Error("Could not cast to integer.");
      }
      return static_cast<T>(r.value());
    }
  }

  static fct::Result<InputArrayType> to_array(InputVarType* _var) noexcept {
    auto r = _var->get_array();
    if (r.error()) {
      return fct::Error("Could not cast to array!");
    }
    return r.value();
  }

  static std::map<std::string, InputVarType> to_map(
      InputObjectType* _obj) noexcept {
    std::map<std::string, InputVarType> m;
    for (auto field : *_obj) {
      std::string_view name = field.unescaped_key();
      m[std::string(name)] = field.value();
    }
    return m;
  }

  static fct::Result<InputObjectType> to_object(InputVarType* _var) noexcept {
    auto r = _var->get_object();
    if (r.error()) {
      return fct::Error("Could not cast to object!");
    }
    return r.value();
  }

  static std::vector<InputVarType> to_vec(InputArrayType* _arr) noexcept {
    std::vector<InputVarType> vec;
    for (auto elem : *_arr) {
      if (!elem.error()) {
        vec.push_back(elem.value());
      }
    }
    return vec;
  }
};

using JSONParser = SimdJSONParser;

template <class T>
using Parser = parsing::Parser<JSONParser, T>;
};  // namespace json

#endif  // JSON_PARSER_HPP_
