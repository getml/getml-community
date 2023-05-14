// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PARSING_PARSER_HPP_
#define PARSING_PARSER_HPP_

#include <cstddef>
#include <exception>
#include <map>
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
#include "fct/TaggedUnion.hpp"
#include "fct/collect.hpp"
#include "fct/field_type.hpp"
#include "fct/find_index.hpp"
#include "fct/iota.hpp"
#include "fct/join.hpp"
#include "parsing/is_required.hpp"
#include "strings/strings.hpp"

namespace parsing {

// ----------------------------------------------------------------------------

template <class ParserType, class T>
struct Parser;

// ----------------------------------------------------------------------------

/// Default case - anything that cannot be explicitly matched.
template <class ParserType, class T>
struct Parser {
  using VarType = typename ParserType::VarType;

  /// Expresses the variables as type T.
  static T from_json(const VarType& _var) {
    if constexpr (json::has_from_json_obj_v<T>) {
      return T::from_json_obj(_var);
    } else {
      if constexpr (fct::has_named_tuple_type_v<T>) {
        using NamedTupleType = std::decay_t<typename T::NamedTupleType>;
        auto named_tuple = Parser<ParserType, NamedTupleType>::from_json(_var);
        return T(named_tuple);
      } else {
        return ParserType::template to_basic_type<std::decay_t<T>>(_var);
      }
    }
  }

  /// Converts the variable to a JSON type.
  static auto to_json(const T& _var) {
    if constexpr (fct::has_named_tuple_type_v<T>) {
      using NamedTupleType = std::decay_t<typename T::NamedTupleType>;
      if constexpr (fct::has_named_tuple_method_v<T>) {
        return Parser<ParserType, NamedTupleType>::to_json(_var.named_tuple());
      } else {
        const auto& [r] = _var;
        return Parser<ParserType, NamedTupleType>::to_json(r);
      }
    } else {
      return _var;
    }
  }
};

// ----------------------------------------------------------------------------

template <class ParserType, fct::StringLiteral... _fields>
struct Parser<ParserType, fct::Literal<_fields...>> {
  using VarType = typename ParserType::VarType;

  /// Expresses the variables as type T.
  static fct::Literal<_fields...> from_json(const VarType& _var) {
    try {
      return fct::Literal<_fields...>(
          ParserType::template to_basic_type<std::string>(_var));
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

template <class ParserType, class ValueType>
struct Parser<ParserType, std::map<std::string, ValueType>> {
  using ObjectType = typename ParserType::ObjectType;
  using VarType = typename ParserType::VarType;

  /// Expresses the variables as a std::map.
  static std::map<std::string, ValueType> from_json(const VarType& _var) {
    const auto obj = ParserType::to_object(_var);
    const auto names = ParserType::get_names(obj);
    const auto get_value =
        [&obj](const auto& _name) -> std::pair<std::string, ValueType> {
      return std::make_pair(
          _name, Parser<ParserType, std::decay_t<ValueType>>::from_json(
                     ParserType::get_field(obj, _name)));
    };
    return fct::collect::map<std::string, ValueType>(
        names | VIEWS::transform(get_value));
  }

  /// Transform a std::vector into an object
  static ObjectType to_json(const std::map<std::string, ValueType>& _m) {
    auto obj = ParserType::new_object();
    for (const auto& [k, v] : _m) {
      ParserType::set_field(
          k, Parser<ParserType, std::decay_t<ValueType>>::to_json(v), &obj);
    }
    return obj;
  }
};

// ----------------------------------------------------------------------------

template <class ParserType, class... FieldTypes>
struct Parser<ParserType, fct::NamedTuple<FieldTypes...>> {
  using ObjectType = typename ParserType::ObjectType;
  using VarType = typename ParserType::VarType;

 public:
  /// Generates a NamedTuple from a JSON Object.
  static fct::NamedTuple<FieldTypes...> from_json(const VarType& _var) {
    const auto obj = ParserType::to_object(_var);
    return build_named_tuple_recursively(obj);
  }

  /// Transforms a NamedTuple into a JSON object.
  static VarType to_json(const fct::NamedTuple<FieldTypes...>& _tup) {
    auto obj = ParserType::new_object();
    return build_object_recursively(_tup, &obj);
  }

 private:
  /// Builds the named tuple field by field.
  template <class... Args>
  static auto build_named_tuple_recursively(const ObjectType& _obj,
                                            const Args&... _args) {
    const auto size = sizeof...(Args);

    if constexpr (size == sizeof...(FieldTypes)) {
      return fct::NamedTuple<Args...>(_args...);
    } else {
      using FieldType = typename std::tuple_element<
          size, typename fct::NamedTuple<FieldTypes...>::Fields>::type;

      const auto key = FieldType::name_.str();

      if (!ParserType::has_key(_obj, key)) {
        using ValueType = std::decay_t<typename FieldType::Type>;
        if constexpr (is_required<ValueType>()) {
          throw std::runtime_error("Field named '" + key + "' not found!");
        } else {
          return build_named_tuple_recursively(_obj, _args...,
                                               FieldType(ValueType()));
        }
      }

      const auto value = get_value<FieldType>(_obj);

      return build_named_tuple_recursively(_obj, _args..., FieldType(value));
    }
  }

  /// Builds the object field by field.
  template <int _i = 0>
  static ObjectType build_object_recursively(
      const fct::NamedTuple<FieldTypes...>& _tup, ObjectType* _ptr) {
    if constexpr (_i >= sizeof...(FieldTypes)) {
      return *_ptr;
    } else {
      using FieldType =
          typename std::tuple_element<_i, std::tuple<FieldTypes...>>::type;
      using ValueType = std::decay_t<typename FieldType::Type>;
      const auto value =
          Parser<ParserType, ValueType>::to_json(fct::get<_i>(_tup));
      const auto name = FieldType::name_.str();
      if constexpr (!is_required<ValueType>()) {
        if (!ParserType::is_empty(value)) {
          ParserType::set_field(name, value, _ptr);
        }
      } else {
        ParserType::set_field(name, value, _ptr);
      }
      return build_object_recursively<_i + 1>(_tup, _ptr);
    }
  }

  /// Retrieves the value from the object. This is mainly needed to generate a
  /// better error message.
  template <class FieldType>
  static auto get_value(const ObjectType& _obj) {
    try {
      using ValueType = std::decay_t<typename FieldType::Type>;
      return Parser<ParserType, ValueType>::from_json(
          ParserType::get_field(_obj, FieldType::name_.str()));
    } catch (std::exception& _exp) {
      throw std::runtime_error("Failed to parse field '" +
                               FieldType::name_.str() + "': " + _exp.what());
    }
  }
};

// ----------------------------------------------------------------------------

template <class ParserType, class T>
struct Parser<ParserType, std::optional<T>> {
  using VarType = typename ParserType::VarType;

  /// Expresses the variables as type T.
  static std::optional<T> from_json(const VarType& _var) {
    if (ParserType::is_empty(_var)) {
      return std::nullopt;
    }
    return std::make_optional<T>(
        Parser<ParserType, std::decay_t<T>>::from_json(_var));
  }

  /// Expresses the variable a a JSON.
  static VarType to_json(const std::optional<T>& _o) {
    if (!_o) {
      return ParserType::empty_var();
    }
    return Parser<ParserType, std::decay_t<T>>::to_json(*_o);
  }
};

// ----------------------------------------------------------------------------

template <class ParserType, class T>
struct Parser<ParserType, fct::Ref<T>> {
  using VarType = typename ParserType::VarType;

  /// Expresses the variables as type T.
  static fct::Ref<T> from_json(const VarType& _var) {
    return fct::Ref<T>::make(
        Parser<ParserType, std::decay_t<T>>::from_json(_var));
  }

  /// Expresses the variable a a JSON.
  static auto to_json(const fct::Ref<T>& _ref) {
    return Parser<ParserType, std::decay_t<T>>::to_json(*_ref);
  }
};

// ----------------------------------------------------------------------------

template <class ParserType, class T>
struct Parser<ParserType, std::shared_ptr<T>> {
  using VarType = typename ParserType::VarType;

  /// Expresses the variables as type T.
  static std::shared_ptr<T> from_json(const VarType& _var) {
    if (ParserType::is_empty(_var)) {
      return nullptr;
    }
    return std::make_shared<T>(
        Parser<ParserType, std::decay_t<T>>::from_json(_var));
  }

  /// Expresses the variable a a JSON.
  static VarType to_json(const std::shared_ptr<T>& _s) {
    if (!_s) {
      return ParserType::empty_var();
    }
    return Parser<ParserType, std::decay_t<T>>::to_json(*_s);
  }
};

// ----------------------------------------------------------------------------

template <class ParserType, class FirstType, class SecondType>
struct Parser<ParserType, std::pair<FirstType, SecondType>> {
  using VarType = typename ParserType::VarType;

  /// Expresses the variables as type T.
  static std::pair<FirstType, SecondType> from_json(const VarType& _var) {
    auto tuple =
        Parser<ParserType, std::tuple<FirstType, SecondType>>::from_json(_var);
    return std::make_pair(std::move(std::get<0>(tuple)),
                          std::move(std::get<1>(tuple)));
  }

  /// Transform a std::vector into an array
  static VarType to_json(const std::pair<FirstType, SecondType>& _p) {
    return Parser<ParserType, std::tuple<FirstType, SecondType>>::to_json(
        std::make_tuple(_p.first, _p.second));
  }
};

// ----------------------------------------------------------------------------

template <class ParserType, class T>
struct Parser<ParserType, std::set<T>> {
  using ArrayType = typename ParserType::ArrayType;
  using VarType = typename ParserType::VarType;

 public:
  /// Expresses the variables as type T.
  static std::set<T> from_json(const VarType& _var) {
    const auto arr = ParserType::to_array(_var);
    const auto iota = fct::iota<size_t>(0, ParserType::get_array_size(arr));
    const auto get_value = [&arr](const size_t _i) {
      try {
        return Parser<ParserType, std::decay_t<T>>::from_json(
            ParserType::get(arr, _i));
      } catch (std::exception& _exp) {
        throw std::runtime_error("Error parsing element " + std::to_string(_i) +
                                 ": " + _exp.what());
      }
    };
    return fct::collect::set(iota | VIEWS::transform(get_value));
  }

  /// Transform a std::vector into an array
  static ArrayType to_json(const std::set<T>& _s) {
    auto arr = ParserType::new_array();
    for (const auto& val : _s) {
      ParserType::add(Parser<ParserType, std::decay_t<T>>::to_json(val), &arr);
    }
    return arr;
  }
};

// ----------------------------------------------------------------------------

template <class ParserType>
struct Parser<ParserType, strings::String> {
  using VarType = typename ParserType::VarType;

  /// Expresses the variables as type T.
  static strings::String from_json(const VarType& _var) {
    return strings::String(Parser<ParserType, std::string>::from_json(_var));
  }

  /// Transform a std::vector into an array
  static VarType to_json(const strings::String& _s) {
    return Parser<ParserType, std::string>::to_json(_s.str());
  }
};

// ----------------------------------------------------------------------------

template <class ParserType, fct::StringLiteral _discriminator,
          class... NamedTupleTypes>
struct Parser<ParserType,
              fct::TaggedUnion<_discriminator, NamedTupleTypes...>> {
 public:
  using ObjectType = typename ParserType::ObjectType;
  using VarType = typename ParserType::VarType;

  /// Expresses the variables as type T.
  static fct::TaggedUnion<_discriminator, NamedTupleTypes...> from_json(
      const VarType& _var) {
    const auto obj = ParserType::to_object(_var);
    const auto disc_value = get_discriminator(obj, _discriminator.str());
    return find_matching_named_tuple(obj, disc_value);
  }

  /// Expresses the variables as a JSON type.
  static VarType to_json(
      const fct::TaggedUnion<_discriminator, NamedTupleTypes...>&
          _tagged_union) {
    using VariantType =
        typename fct::TaggedUnion<_discriminator,
                                  NamedTupleTypes...>::VariantType;
    return Parser<ParserType, VariantType>::to_json(_tagged_union.variant_);
  }

 private:
  template <int _i = 0>
  static fct::TaggedUnion<_discriminator, NamedTupleTypes...>
  find_matching_named_tuple(const ObjectType& _obj,
                            const std::string& _disc_value) {
    if constexpr (_i == sizeof...(NamedTupleTypes)) {
      throw std::runtime_error(
          "Could not parse tagged union, could not match " +
          _discriminator.str() + " '" + _disc_value + "'.");
    } else {
      const auto optional = try_option<_i>(_obj, _disc_value);
      if (optional) {
        return *optional;
      } else {
        return find_matching_named_tuple<_i + 1>(_obj, _disc_value);
      }
    }
  }

  /// Retrieves the discriminator.
  static std::string get_discriminator(const ObjectType& _obj,
                                       const std::string& _field_name) {
    try {
      return ParserType::template to_basic_type<std::string>(
          ParserType::get_field(_obj, _field_name));
    } catch (std::exception& _e) {
      throw std::runtime_error(
          "Could not parse tagged union: Could not find field " + _field_name +
          " or type of field was not a string.");
    }
  }

  /// Determines whether the discriminating literal contains the value
  /// retrieved from the object.
  template <class T>
  static inline bool contains_disc_value(const std::string& _disc_value) {
    if constexpr (!fct::has_named_tuple_type_v<T>) {
      using LiteralType = fct::field_type_t<_discriminator, T>;
      return LiteralType::contains(_disc_value);
    } else {
      using LiteralType =
          fct::field_type_t<_discriminator, typename T::NamedTupleType>;
      return LiteralType::contains(_disc_value);
    }
  }

  /// Tries to parse one particular option.
  template <int _i>
  static std::optional<fct::TaggedUnion<_discriminator, NamedTupleTypes...>>
  try_option(const ObjectType& _obj, const std::string& _disc_value) {
    using NamedTupleType = std::decay_t<
        std::variant_alternative_t<_i, std::variant<NamedTupleTypes...>>>;

    if (contains_disc_value<NamedTupleType>(_disc_value)) {
      try {
        auto val = Parser<ParserType, NamedTupleType>::from_json(_obj);
        return fct::TaggedUnion<_discriminator, NamedTupleTypes...>(val);
      } catch (std::exception& _e) {
        throw std::runtime_error(
            "Could not parse tagged union with discrimininator " +
            _discriminator.str() + " '" + _disc_value + "': " + _e.what());
      }
    } else {
      return std::nullopt;
    }
  }
};

// ----------------------------------------------------------------------------

template <class ParserType, class... Ts>
struct Parser<ParserType, std::tuple<Ts...>> {
 public:
  using ArrayType = typename ParserType::ArrayType;
  using VarType = typename ParserType::VarType;

  /// Expresses the variables as type T.
  static std::tuple<Ts...> from_json(const VarType& _var) {
    const auto arr = ParserType::to_array(_var);
    if (ParserType::get_array_size(arr) != sizeof...(Ts)) {
      throw std::runtime_error(
          "Expected " + std::to_string(sizeof...(Ts)) + " fields, got " +
          std::to_string(ParserType::get_array_size(arr)) + ".");
    }
    return extract_field_by_field(arr);
  }

  /// Transform a std::vector into a array
  static VarType to_json(const std::tuple<Ts...>& _tup) {
    auto arr = ParserType::new_array();
    to_array<0>(_tup, &arr);
    return arr;
  }

 private:
  /// Extracts values from the array, field by field.
  template <class... AlreadyExtracted>
  static std::tuple<Ts...> extract_field_by_field(
      const ArrayType& _arr, const AlreadyExtracted&... _already_extracted) {
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
  static auto extract_single_field(const ArrayType& _arr) {
    using NewFieldType =
        std::decay_t<typename std::tuple_element<_i, std::tuple<Ts...>>::type>;
    try {
      return Parser<ParserType, NewFieldType>::from_json(
          ParserType::get(_arr, _i));
    } catch (std::exception& _exp) {
      throw std::runtime_error("Error parsing element " + std::to_string(_i) +
                               ": " + _exp.what());
    }
  }

  /// Transforms a tuple to an array.
  template <int _i>
  static void to_array(const std::tuple<Ts...>& _tup, ArrayType* _ptr) {
    if constexpr (_i < sizeof...(Ts)) {
      assert_true(_ptr);
      using NewFieldType = std::decay_t<
          typename std::tuple_element<_i, std::tuple<Ts...>>::type>;
      ParserType::add(
          Parser<ParserType, NewFieldType>::to_json(std::get<_i>(_tup)), _ptr);
      to_array<_i + 1>(_tup, _ptr);
    }
  }
};

// ----------------------------------------------------------------------------

template <class ParserType, class... FieldTypes>
struct Parser<ParserType, std::variant<FieldTypes...>> {
  using VarType = typename ParserType::VarType;

  /// Expresses the variables as type T.
  template <int _i = 0>
  static std::variant<FieldTypes...> from_json(
      const VarType& _var, const std::vector<std::string> _errors = {}) {
    if constexpr (_i == sizeof...(FieldTypes)) {
      throw std::runtime_error("Could not parse variant: " +
                               fct::collect::string(_errors));
    } else {
      try {
        using AltType = std::decay_t<
            std::variant_alternative_t<_i, std::variant<FieldTypes...>>>;
        return Parser<ParserType, AltType>::from_json(_var);
      } catch (std::exception& e) {
        const auto errors = fct::join::vector<std::string>(
            {_errors,
             std::vector<std::string>({std::string("\n -") + e.what()})});
        return from_json<_i + 1>(_var, errors);
      }
    }
  }

  /// Expresses the variables as a JSON type.
  template <int _i = 0>
  static VarType to_json(const std::variant<FieldTypes...>& _variant) {
    using VarType = std::variant_alternative_t<_i, std::variant<FieldTypes...>>;
    if constexpr (_i + 1 == sizeof...(FieldTypes)) {
      return Parser<ParserType, std::decay_t<VarType>>::to_json(
          std::get<VarType>(_variant));
    } else {
      if (std::holds_alternative<VarType>(_variant)) {
        return Parser<ParserType, std::decay_t<VarType>>::to_json(
            std::get<VarType>(_variant));
      } else {
        return to_json<_i + 1>(_variant);
      }
    }
  }
};

// ----------------------------------------------------------------------------

template <class ParserType, class T>
struct Parser<ParserType, std::vector<T>> {
 public:
  using ArrayType = typename ParserType::ArrayType;
  using VarType = typename ParserType::VarType;

  /// Expresses the variables as type T.
  static std::vector<T> from_json(const VarType& _var) {
    const auto arr = ParserType::to_array(_var);
    const auto iota = fct::iota<size_t>(0, ParserType::get_array_size(arr));
    const auto get_value = [&arr](const size_t _i) {
      try {
        return Parser<ParserType, std::decay_t<T>>::from_json(
            ParserType::get(arr, _i));
      } catch (std::exception& _exp) {
        throw std::runtime_error("Error parsing element " + std::to_string(_i) +
                                 ": " + _exp.what());
      }
    };
    return fct::collect::vector(iota | VIEWS::transform(get_value));
  }

  /// Transform a std::vector into an array
  static ArrayType to_json(const std::vector<T>& _vec) {
    auto arr = ParserType::new_array();
    for (size_t i = 0; i < _vec.size(); ++i) {
      ParserType::add(Parser<ParserType, std::decay_t<T>>::to_json(_vec[i]),
                      &arr);
    }
    return arr;
  }
};

}  // namespace parsing

#endif  // JSON_PARSER_HPP_
