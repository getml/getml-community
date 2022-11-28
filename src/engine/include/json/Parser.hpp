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

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <vector>

#include "fct/NamedTuple.hpp"
#include "fct/collect.hpp"
#include "fct/iota.hpp"

namespace json {

// ----------------------------------------------------------------------------

template <class T>
struct Parser;

// ----------------------------------------------------------------------------

/// Default case - anything that cannot be explicitly matched.
template <class T>
struct Parser {
  /// Expresses the variables as type T.
  static T from_json(const Poco::Dynamic::Var& _var) {
    return _var.convert<T>();
  }

  /// Does nothing, exists for compatability reason.
  static T to_json(const T& _var) { return _var; }
};

// ----------------------------------------------------------------------------

template <class... FieldTypes>
struct Parser<fct::NamedTuple<FieldTypes...>> {
 public:
  /// Generates a NamedTuple from a JSON Object.
  static fct::NamedTuple<FieldTypes...> from_json(
      const Poco::Dynamic::Var& _var) {
    const auto obj = get_object(_var);
    return build_named_tuple_recursively(obj);
  }

  /// Transforms a NamedTuple into a JSON object.
  static Poco::JSON::Object::Ptr to_json(
      const fct::NamedTuple<FieldTypes...>& _tup) {
    auto ptr = Poco::JSON::Object::Ptr(new Poco::JSON::Object());
    return build_object_recursively(_tup, ptr);
  }

 private:
  /// Builds the named tuple field by field.
  template <class... Args>
  static auto build_named_tuple_recursively(const Poco::JSON::Object& _obj,
                                            const Args&... _args) {
    const auto size = sizeof...(Args);

    if constexpr (size == sizeof...(FieldTypes)) {
      return fct::NamedTuple<Args...>(_args...);
    } else {
      using FieldType = typename std::tuple_element<
          size, typename fct::NamedTuple<FieldTypes...>::Fields>::type;

      const auto key = FieldType::name_.str();

      if (!_obj.has(key)) {
        throw std::runtime_error("Field named '" + key + "' not found!");
      }

      const auto value = get_value<FieldType>(_obj);

      return build_named_tuple_recursively(_obj, _args..., FieldType(value));
    }
  }

  /// Builds the object field by field.
  template <int _i = 0>
  static Poco::JSON::Object::Ptr build_object_recursively(
      const fct::NamedTuple<FieldTypes...>& _tup,
      Poco::JSON::Object::Ptr _ptr) {
    if constexpr (_i >= sizeof...(FieldTypes)) {
      return _ptr;
    } else {
      using FieldType =
          typename std::tuple_element<_i, std::tuple<FieldTypes...>>::type;
      using ValueType = typename FieldType::Type;
      const auto value = Parser<ValueType>::to_json(fct::get<_i>(_tup));
      const auto name = FieldType::name_.str();
      _ptr->set(name, value);
      return build_object_recursively<_i + 1>(_tup, _ptr);
    }
  }

  /// Retrieves the object from the dynamic var.
  static Poco::JSON::Object get_object(const Poco::Dynamic::Var& _var) {
    try {
      return _var.extract<Poco::JSON::Object>();

    } catch (std::exception& _exp) {
    }

    try {
      const auto ptr = _var.extract<Poco::JSON::Object::Ptr>();

      if (!ptr) {
        throw std::runtime_error("Poco::JSON::Object::Ptr is NULL.");
      }

      return *ptr;
    } catch (std::exception& _exp) {
      throw std::runtime_error(
          "Expected a Poco::JSON::Object or a Poco::JSON::Object::Ptr.");
    }
  }

  /// Retrieves the value from the object. This is mainly needed to generate a
  /// better error message.
  template <class FieldType>
  static auto get_value(const Poco::JSON::Object& _obj) {
    try {
      return Parser<typename FieldType::Type>::from_json(
          _obj.get(FieldType::name_.str()));
    } catch (std::exception& _exp) {
      throw std::runtime_error("Failed to parse JSON field '" +
                               FieldType::name_.str() + "': " + _exp.what());
    }
  }
};

// ----------------------------------------------------------------------------

template <class T>
struct Parser<std::optional<T>> {
  /// Expresses the variables as type T.
  static std::optional<T> from_json(const Poco::Dynamic::Var& _var) {
    return std::make_optional(Parser<T>::from_json(_var));
  }
};

// ----------------------------------------------------------------------------

template <class T>
struct Parser<fct::Ref<T>> {
  /// Expresses the variables as type T.
  static fct::Ref<T> from_json(const Poco::Dynamic::Var& _var) {
    return fct::Ref<T>::make(Parser<T>::from_json(_var));
  }
};

// ----------------------------------------------------------------------------

template <class T>
struct Parser<std::shared_ptr<T>> {
  /// Expresses the variables as type T.
  static std::shared_ptr<T> from_json(const Poco::Dynamic::Var& _var) {
    return std::make_shared<T>(Parser<T>::from_json(_var));
  }
};

// ----------------------------------------------------------------------------

template <class T>
struct Parser<std::vector<T>> {
 public:
  /// Expresses the variables as type T.
  static std::vector<T> from_json(const Poco::Dynamic::Var& _var) {
    const auto arr = get_array(_var);
    const auto iota = fct::iota<size_t>(0, arr.size());
    const auto get_value = [&arr](const size_t _i) {
      try {
        return Parser<T>::from_json(arr.get(_i));
      } catch (std::exception& _exp) {
        throw std::runtime_error("Error parsing element " + std::to_string(_i) +
                                 ": " + _exp.what());
      }
    };
    return fct::collect::vector<T>(iota | VIEWS::transform(get_value));
  }

  /// Transform a std::vector into a Poco::JSON::Array
  static Poco::JSON::Array::Ptr to_json(const std::vector<T>& _vec) {
    auto ptr = Poco::JSON::Array::Ptr(new Poco::JSON::Array());
    for (size_t i = 0; i < _vec.size(); ++i) {
      ptr->add(Parser<T>::to_json(_vec[i]));
    }
    return ptr;
  }

 private:
  /// Retrieves the array from the dynamic var.
  static Poco::JSON::Array get_array(const Poco::Dynamic::Var& _var) {
    try {
      return _var.extract<Poco::JSON::Array>();

    } catch (std::exception& _exp) {
    }

    try {
      const auto ptr = _var.extract<Poco::JSON::Array::Ptr>();

      if (!ptr) {
        throw std::runtime_error("Poco::JSON::Array::Ptr is NULL.");
      }

      return *ptr;
    } catch (std::exception& _exp) {
      throw std::runtime_error(
          "Expected a Poco::JSON::Array or a Poco::JSON::Array::Ptr.");
    }
  }
};

// ----------------------------------------------------------------------------

}  // namespace json

#endif  // JSON_PARSER_HPP_
