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
#include <exception>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <variant>
#include <vector>

#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "fct/StringLiteral.hpp"
#include "fct/collect.hpp"
#include "fct/iota.hpp"
#include "fct/join.hpp"
#include "json/ArrayGetter.hpp"
#include "strings/strings.hpp"

namespace json {

// ----------------------------------------------------------------------------

template <class T>
struct Parser;

// ----------------------------------------------------------------------------

/// Default case - anything that cannot be explicitly matched.
template <class T>
struct Parser {
  /// Expresses the variables as type T.
  static auto from_json(const Poco::Dynamic::Var& _var) {
    if constexpr (fct::has_named_tuple_type_v<T>) {
      using NamedTupleType = std::decay_t<typename T::NamedTupleType>;
      return T(Parser<NamedTupleType>::from_json(_var));
    } else {
      return _var.convert<std::decay_t<T>>();
    }
  }

  /// Converts the variable to a JSON type.
  static auto to_json(const T& _var) {
    if constexpr (fct::has_named_tuple_type_v<T>) {
      using NamedTupleType = std::decay_t<typename T::NamedTupleType>;
      if constexpr (fct::has_named_tuple_method_v<T>) {
        return Parser<NamedTupleType>::to_json(_var.named_tuple());
      } else {
        const auto& [r] = _var;
        return Parser<NamedTupleType>::to_json(r);
      }
    } else {
      return _var;
    }
  }
};

// ----------------------------------------------------------------------------

template <fct::StringLiteral... _fields>
struct Parser<fct::Literal<_fields...>> {
  /// Expresses the variables as type T.
  static fct::Literal<_fields...> from_json(const Poco::Dynamic::Var& _var) {
    try {
      return fct::Literal<_fields...>(_var.convert<std::string>());
    } catch (std::exception& _e) {
      throw std::runtime_error(std::string("Failed to parse Literal: ") +
                               _e.what());
    }
  }

  /// Expresses the variable a a JSON.
  static auto to_json(const fct::Literal<_fields...>& _literal) {
    return _literal.name();
  }
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
      using ValueType = std::decay_t<typename FieldType::Type>;
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
      using ValueType = std::decay_t<typename FieldType::Type>;
      return Parser<ValueType>::from_json(_obj.get(FieldType::name_.str()));
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
    if (_var.isEmpty()) {
      return std::nullopt;
    }
    return std::make_optional<T>(Parser<std::decay_t<T>>::from_json(_var));
  }

  /// Expresses the variable a a JSON.
  static Poco::Dynamic::Var to_json(const std::optional<T>& _o) {
    if (!_o) {
      return Poco::Dynamic::Var();
    }
    return Parser<std::decay_t<T>>::to_json(*_o);
  }
};

// ----------------------------------------------------------------------------

template <class T>
struct Parser<fct::Ref<T>> {
  /// Expresses the variables as type T.
  static fct::Ref<T> from_json(const Poco::Dynamic::Var& _var) {
    return fct::Ref<T>::make(Parser<std::decay_t<T>>::from_json(_var));
  }

  /// Expresses the variable a a JSON.
  static auto to_json(const fct::Ref<T>& _ref) {
    return Parser<std::decay_t<T>>::to_json(*_ref);
  }
};

// ----------------------------------------------------------------------------

template <class T>
struct Parser<std::shared_ptr<T>> {
  /// Expresses the variables as type T.
  static std::shared_ptr<T> from_json(const Poco::Dynamic::Var& _var) {
    if (_var.isEmpty()) {
      return nullptr;
    }
    return std::make_shared<T>(Parser<std::decay_t<T>>::from_json(_var));
  }

  /// Expresses the variable a a JSON.
  static Poco::Dynamic::Var to_json(const std::shared_ptr<T>& _s) {
    if (!_s) {
      return Poco::Dynamic::Var();
    }
    return Parser<std::decay_t<T>>::to_json(*_s);
  }
};

// ----------------------------------------------------------------------------

template <class FirstType, class SecondType>
struct Parser<std::pair<FirstType, SecondType>> {
  /// Expresses the variables as type T.
  static std::pair<FirstType, SecondType> from_json(
      const Poco::Dynamic::Var& _var) {
    auto tuple = Parser<std::tuple<FirstType, SecondType>>::from_json(_var);
    return std::make_pair(std::move(std::get<0>(tuple)),
                          std::move(std::get<1>(tuple)));
  }

  /// Transform a std::vector into a Poco::JSON::Array
  static Poco::Dynamic::Var to_json(
      const std::pair<FirstType, SecondType>& _p) {
    return Parser<std::tuple<FirstType, SecondType>>::to_json(
        std::make_tuple(_p.first, _p.second));
  }
};

// ----------------------------------------------------------------------------

template <class T>
struct Parser<std::set<T>> {
 public:
  /// Expresses the variables as type T.
  static std::set<T> from_json(const Poco::Dynamic::Var& _var) {
    const auto arr = ArrayGetter::get_array(_var);
    const auto iota = fct::iota<size_t>(0, arr.size());
    const auto get_value = [&arr](const size_t _i) {
      try {
        return Parser<std::decay_t<T>>::from_json(arr.get(_i));
      } catch (std::exception& _exp) {
        throw std::runtime_error("Error parsing element " + std::to_string(_i) +
                                 ": " + _exp.what());
      }
    };
    return fct::collect::set<T>(iota | VIEWS::transform(get_value));
  }

  /// Transform a std::vector into a Poco::JSON::Array
  static Poco::JSON::Array::Ptr to_json(const std::set<T>& _s) {
    auto ptr = Poco::JSON::Array::Ptr(new Poco::JSON::Array());
    for (const auto& val : _s) {
      ptr->add(Parser<std::decay_t<T>>::to_json(val));
    }
    return ptr;
  }
};

// ----------------------------------------------------------------------------

template <>
struct Parser<strings::String> {
  /// Expresses the variables as type T.
  static strings::String from_json(const Poco::Dynamic::Var& _var) {
    return strings::String(Parser<std::string>::from_json(_var));
  }

  /// Transform a std::vector into a Poco::JSON::Array
  static Poco::Dynamic::Var to_json(const strings::String& _s) {
    return Parser<std::string>::to_json(_s.str());
  }
};

// ----------------------------------------------------------------------------

template <class... Ts>
struct Parser<std::tuple<Ts...>> {
 public:
  /// Expresses the variables as type T.
  static std::tuple<Ts...> from_json(const Poco::Dynamic::Var& _var) {
    const auto arr = ArrayGetter::get_array(_var);
    if (arr.size() != sizeof...(Ts)) {
      throw std::runtime_error("Expected " + std::to_string(sizeof...(Ts)) +
                               " fields, got " + std::to_string(arr.size()) +
                               ".");
    }
    return extract_field_by_field(arr);
  }

  /// Transform a std::vector into a Poco::JSON::Array
  static Poco::Dynamic::Var to_json(const std::tuple<Ts...>& _tup) {
    auto ptr = Poco::JSON::Array::Ptr(new Poco::JSON::Array());
    to_array<0>(_tup, ptr);
    return ptr;
  }

 private:
  /// Extracts values from the array, field by field.
  template <class... AlreadyExtracted>
  static std::tuple<Ts...> extract_field_by_field(
      const Poco::JSON::Array& _arr,
      const AlreadyExtracted&... _already_extracted) {
    constexpr size_t i = sizeof...(AlreadyExtracted);
    if constexpr (i == sizeof...(Ts)) {
      return std::make_tuple(_already_extracted...);
    } else {
      const auto new_entry = extract_single_field<i>(_arr);
      return extract_field_by_field(_arr, _already_extracted..., new_entry);
    }
  }

  /// Extracts a single field from a JSON.
  template <int _i>
  static auto extract_single_field(const Poco::JSON::Array& _arr) {
    using NewFieldType =
        std::decay_t<typename std::tuple_element<_i, std::tuple<Ts...>>::type>;
    try {
      return Parser<NewFieldType>::from_json(_arr.get(_i));
    } catch (std::exception& _exp) {
      throw std::runtime_error("Error parsing element " + std::to_string(_i) +
                               ": " + _exp.what());
    }
  }

  /// Transforms a tuple to an array.
  template <int _i>
  static void to_array(const std::tuple<Ts...>& _tup,
                       Poco::JSON::Array::Ptr& _ptr) {
    if constexpr (_i < sizeof...(Ts)) {
      assert_true(_ptr);
      using NewFieldType = std::decay_t<
          typename std::tuple_element<_i, std::tuple<Ts...>>::type>;
      _ptr->add(Parser<NewFieldType>::to_json(std::get<_i>(_tup)));
      to_array<_i + 1>(_tup, _ptr);
    }
  }
};

// ----------------------------------------------------------------------------

template <class... FieldTypes>
struct Parser<std::variant<FieldTypes...>> {
  /// Expresses the variables as type T.
  template <int _i = 0>
  static std::variant<FieldTypes...> from_json(
      const Poco::Dynamic::Var& _var,
      const std::vector<std::string> _errors = {}) {
    if constexpr (_i == sizeof...(FieldTypes)) {
      throw std::runtime_error("Could not parse variant: " +
                               fct::collect::string(_errors));
    } else {
      try {
        using VarType = std::decay_t<
            std::variant_alternative_t<_i, std::variant<FieldTypes...>>>;
        return Parser<VarType>::from_json(_var);
      } catch (std::exception& e) {
        const auto errors = fct::join::vector<std::string>(
            {_errors, std::vector<std::string>({e.what()})});
        return from_json<_i + 1>(_var, errors);
      }
    }
  }

  /// Expresses the variables as a JSON type.
  template <int _i = 0>
  static Poco::Dynamic::Var to_json(
      const std::variant<FieldTypes...>& _variant) {
    using VarType = std::variant_alternative_t<_i, std::variant<FieldTypes...>>;
    if constexpr (_i + 1 == sizeof...(FieldTypes)) {
      return Parser<std::decay_t<VarType>>::to_json(
          std::get<VarType>(_variant));
    } else {
      if (std::holds_alternative<VarType>(_variant)) {
        return Parser<std::decay_t<VarType>>::to_json(
            std::get<VarType>(_variant));
      } else {
        return to_json<_i + 1>(_variant);
      }
    }
  }
};

// ----------------------------------------------------------------------------

template <class T>
struct Parser<std::vector<T>> {
 public:
  /// Expresses the variables as type T.
  static std::vector<T> from_json(const Poco::Dynamic::Var& _var) {
    const auto arr = ArrayGetter::get_array(_var);
    const auto iota = fct::iota<size_t>(0, arr.size());
    const auto get_value = [&arr](const size_t _i) {
      try {
        return Parser<std::decay_t<T>>::from_json(arr.get(_i));
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
      ptr->add(Parser<std::decay_t<T>>::to_json(_vec[i]));
    }
    return ptr;
  }
};

// ----------------------------------------------------------------------------

}  // namespace json

#endif  // JSON_PARSER_HPP_
