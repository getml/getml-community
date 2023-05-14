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

#include <exception>
#include <string>
#include <type_traits>
#include <vector>

#include "json/has_from_json_obj.hpp"
#include "parsing/Parser.hpp"

namespace json {

struct PocoJSONParser {
  using ArrayType = Poco::JSON::Array::Ptr;
  using ObjectType = Poco::JSON::Object::Ptr;
  using VarType = Poco::Dynamic::Var;

  static void add(const VarType _var, ArrayType* _arr) {
    assert_true(_arr);
    assert_true(*_arr);
    (*_arr)->add(_var);
  }

  static VarType empty_var() { return Poco::Dynamic::Var(); }

  static VarType get(const ArrayType& _arr, const size_t _i) {
    assert_true(_arr);
    return _arr->get(_i);
  }

  static size_t get_array_size(const ArrayType& _arr) {
    assert_true(_arr);
    return _arr->size();
  }

  static VarType get_field(const ObjectType& _obj, const std::string& _name) {
    assert_true(_obj);
    return _obj->get(_name);
  }

  static auto get_names(const ObjectType& _obj) { return _obj->getNames(); }

  static bool has_key(const ObjectType& _obj, const std::string& _key) {
    assert_true(_obj);
    return _obj->has(_key);
  }

  static ArrayType new_array() {
    return Poco::JSON::Array::Ptr(new Poco::JSON::Array());
  }

  static ObjectType new_object() {
    return Poco::JSON::Object::Ptr(new Poco::JSON::Object());
  }

  static bool is_empty(const VarType& _var) { return _var.isEmpty(); }

  static void set_field(const std::string& _name, const VarType& _var,
                        ObjectType* _obj) {
    assert_true(_obj);
    assert_true(*_obj);
    (*_obj)->set(_name, _var);
  }

  template <class T>
  static T to_basic_type(const VarType& _var) {
    try {
      return _var.convert<std::decay_t<T>>();
    } catch (std::exception& e) {
      throw std::runtime_error(
          "Could not cast to expected type. Contained value was: " +
          _var.toString());
    }
  }

  static ArrayType to_array(const VarType& _var) {
    auto arr = _var.extract<ArrayType>();
    if (!arr) {
      throw std::runtime_error("Could not retrieve array!");
    }
    return arr;
  }

  static ObjectType to_object(const VarType& _var) {
    auto obj = _var.extract<ObjectType>();
    if (!obj) {
      throw std::runtime_error("Could not retrieve object!");
    }
    return obj;
  }
};

using JSONParser = PocoJSONParser;

template <class T>
using Parser = parsing::Parser<JSONParser, T>;
};  // namespace json

#endif  // JSON_PARSER_HPP_
