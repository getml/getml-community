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
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "fct/Result.hpp"
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

  static void add(const OutputVarType _var, OutputArrayType* _arr) {
    assert_true(_arr);
    assert_true(*_arr);
    (*_arr)->add(_var);
  }

  static OutputVarType empty_var() { return Poco::Dynamic::Var(); }

  static InputVarType get(const size_t _i, const InputArrayType* _arr) {
    assert_true(_arr);
    assert_true(*_arr);
    return (*_arr)->get(_i);
  }

  static size_t get_array_size(InputArrayType* _arr) {
    assert_true(_arr);
    assert_true(*_arr);
    return (*_arr)->size();
  }

  static InputVarType get_field(const std::string& _name,
                                InputObjectType* _obj) {
    assert_true(_obj);
    assert_true(*_obj);
    return (*_obj)->get(_name);
  }

  static auto get_names(InputObjectType* _obj) { return (*_obj)->getNames(); }

  static bool has_key(const std::string& _key, InputObjectType* _obj) {
    assert_true(_obj);
    assert_true(*_obj);
    return (*_obj)->has(_key);
  }

  static OutputArrayType new_array() {
    return Poco::JSON::Array::Ptr(new Poco::JSON::Array());
  }

  static OutputObjectType new_object() {
    return Poco::JSON::Object::Ptr(new Poco::JSON::Object());
  }

  static bool is_empty(InputVarType* _var) {
    assert_true(_var);
    return _var->isEmpty();
  }

  static void set_field(const std::string& _name, const OutputVarType& _var,
                        OutputObjectType* _obj) {
    assert_true(_obj);
    assert_true(*_obj);
    (*_obj)->set(_name, _var);
  }

  template <class T>
  static T to_basic_type(InputVarType* _var) {
    try {
      assert_true(_var);
      assert_true(*_var);
      return _var->convert<std::decay_t<T>>();
    } catch (std::exception& e) {
      throw std::runtime_error(
          "Could not cast to expected type. Contained value was: " +
          _var->toString());
    }
  }

  static InputArrayType to_array(InputVarType* _var) {
    auto arr = _var->extract<InputArrayType>();
    if (!arr) {
      throw std::runtime_error("Could not retrieve array!");
    }
    return arr;
  }

  static InputObjectType to_object(InputVarType* _var) {
    auto obj = _var->extract<InputObjectType>();
    if (!obj) {
      throw std::runtime_error("Could not retrieve object!");
    }
    return obj;
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
