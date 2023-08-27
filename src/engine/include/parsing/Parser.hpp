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
#include "fct/collect_results.hpp"
#include "fct/field_type.hpp"
#include "fct/join.hpp"
#include "parsing/is_required.hpp"
#include "strings/strings.hpp"

namespace parsing {

// ----------------------------------------------------------------------------

template <class ReaderType, class WriterType, class T>
struct Parser;

// ----------------------------------------------------------------------------

/// Default case - anything that cannot be explicitly matched.
template <class ReaderType, class WriterType, class T>
struct Parser {
  using InputVarType = typename ReaderType::InputVarType;
  using OutputVarType = typename WriterType::OutputVarType;

  /// Expresses the variables as type T.
  static fct::Result<T> from_json(const ReaderType& _r,
                                  InputVarType* _var) noexcept {
    if constexpr (ReaderType::template has_custom_constructor<T>) {
      return _r.template use_custom_constructor<T>(_var);
    } else {
      if constexpr (fct::has_named_tuple_type_v<T>) {
        using NamedTupleType = std::decay_t<typename T::NamedTupleType>;
        const auto wrap_in_t = [](auto _named_tuple) {
          return T(_named_tuple);
        };
        return Parser<ReaderType, WriterType, NamedTupleType>::from_json(_r,
                                                                         _var)
            .transform(wrap_in_t);
      } else {
        return _r.template to_basic_type<std::decay_t<T>>(_var);
      }
    }
  }

  /// Converts the variable to a JSON type.
  static auto to_json(const WriterType& _w, const T& _var) noexcept {
    if constexpr (fct::has_named_tuple_type_v<T>) {
      using NamedTupleType = std::decay_t<typename T::NamedTupleType>;
      if constexpr (fct::has_named_tuple_method_v<T>) {
        return Parser<ReaderType, WriterType, NamedTupleType>::to_json(
            _w, _var.named_tuple());
      } else {
        const auto& [r] = _var;
        return Parser<ReaderType, WriterType, NamedTupleType>::to_json(_w, r);
      }
    } else {
      return _w.from_basic_type(_var);
    }
  }
};

// ----------------------------------------------------------------------------

template <class ReaderType, class WriterType, fct::StringLiteral... _fields>
struct Parser<ReaderType, WriterType, fct::Literal<_fields...>> {
  using InputVarType = typename ReaderType::InputVarType;
  using OutputVarType = typename WriterType::OutputVarType;

  /// Expresses the variables as type T.
  static fct::Result<fct::Literal<_fields...>> from_json(
      const ReaderType& _r, InputVarType* _var) noexcept {
    const auto to_type = [](const auto& _str) {
      return fct::Literal<_fields...>::from_string(_str);
    };

    const auto embellish_error = [](const fct::Error& _e) {
      return fct::Error(std::string("Failed to parse Literal: ") + _e.what());
    };

    return _r.template to_basic_type<std::string>(_var)
        .and_then(to_type)
        .or_else(embellish_error);
  }

  /// Expresses the variable a a JSON.
  static OutputVarType to_json(
      const WriterType& _w, const fct::Literal<_fields...>& _literal) noexcept {
    return _w.from_basic_type(_literal.name());
  }
};

// ----------------------------------------------------------------------------

template <class ReaderType, class WriterType, class ValueType>
struct Parser<ReaderType, WriterType, std::map<std::string, ValueType>> {
 public:
  using InputObjectType = typename ReaderType::InputObjectType;
  using InputVarType = typename ReaderType::InputVarType;

  using OutputObjectType = typename WriterType::OutputObjectType;
  using OutputVarType = typename WriterType::OutputVarType;

  /// Expresses the variables as a std::map.
  static fct::Result<std::map<std::string, ValueType>> from_json(
      const ReaderType& _r, InputVarType* _var) noexcept {
    const auto get_pair = [&](auto _pair) {
      const auto to_pair = [&](const ValueType& _val) {
        return std::make_pair(_pair.first, _val);
      };
      return Parser<ReaderType, WriterType, std::decay_t<ValueType>>::from_json(
                 _r, &_pair.second)
          .transform(to_pair);
    };

    const auto to_map = [&_r, get_pair](auto _obj) {
      auto m = _r.to_map(&_obj);
      return fct::collect_results::map(m | VIEWS::transform(get_pair));
    };

    return _r.to_object(_var).and_then(to_map);
  }

  /// Transform a std::vector into an object
  static OutputVarType to_json(
      const WriterType& _w,
      const std::map<std::string, ValueType>& _m) noexcept {
    auto obj = _w.new_object();
    for (const auto& [k, v] : _m) {
      _w.set_field(
          k,
          Parser<ReaderType, WriterType, std::decay_t<ValueType>>::to_json(_w,
                                                                           v),
          &obj);
    }
    return obj;
  }
};

// ----------------------------------------------------------------------------

template <class ReaderType, class WriterType, class... FieldTypes>
struct Parser<ReaderType, WriterType, fct::NamedTuple<FieldTypes...>> {
  using InputObjectType = typename ReaderType::InputObjectType;
  using InputVarType = typename ReaderType::InputVarType;

  using OutputObjectType = typename WriterType::OutputObjectType;
  using OutputVarType = typename WriterType::OutputVarType;

 public:
  /// Generates a NamedTuple from a JSON Object.
  static fct::Result<fct::NamedTuple<FieldTypes...>> from_json(
      const ReaderType& _r, InputVarType* _var) noexcept {
    const auto to_map = [&](auto _obj) { return _r.to_map(&_obj); };
    const auto build = [&](const auto& _map) {
      return build_named_tuple_recursively(_r, _map);
    };
    return _r.to_object(_var).transform(to_map).and_then(build);
  }

  /// Transforms a NamedTuple into a JSON object.
  static OutputVarType to_json(
      const WriterType& _w,
      const fct::NamedTuple<FieldTypes...>& _tup) noexcept {
    auto obj = _w.new_object();
    build_object_recursively(_w, _tup, &obj);
    return OutputVarType(obj);
  }

 private:
  /// Builds the named tuple field by field.
  template <class... Args>
  static fct::Result<fct::NamedTuple<FieldTypes...>>
  build_named_tuple_recursively(const ReaderType& _r,
                                const std::map<std::string, InputVarType>& _map,
                                Args&&... _args) noexcept {
    const auto size = sizeof...(Args);

    if constexpr (size == sizeof...(FieldTypes)) {
      return fct::NamedTuple<FieldTypes...>(_args...);
    } else {
      using FieldType = typename std::tuple_element<
          size, typename fct::NamedTuple<FieldTypes...>::Fields>::type;

      using ValueType = std::decay_t<typename FieldType::Type>;

      const auto key = FieldType::name_.str();

      const auto it = _map.find(key);

      if (it == _map.end()) {
        if constexpr (is_required<ValueType>()) {
          return fct::Error("Field named '" + key + "' not found!");
        } else {
          return build_named_tuple_recursively(_r, _map, _args...,
                                               FieldType(ValueType()));
        }
      }

      const auto build = [&](auto&& _value) {
        return build_named_tuple_recursively(_r, _map, _args...,
                                             FieldType(_value));
      };

      return get_value<ValueType>(_r, *it).and_then(build);
    }
  }

  /// Builds the object field by field.
  template <int _i = 0>
  static void build_object_recursively(
      const WriterType& _w, const fct::NamedTuple<FieldTypes...>& _tup,
      OutputObjectType* _ptr) noexcept {
    if constexpr (_i >= sizeof...(FieldTypes)) {
      return;
    } else {
      using FieldType =
          typename std::tuple_element<_i, std::tuple<FieldTypes...>>::type;
      using ValueType = std::decay_t<typename FieldType::Type>;
      auto value = Parser<ReaderType, WriterType, ValueType>::to_json(
          _w, fct::get<_i>(_tup));
      const auto name = FieldType::name_.str();
      if constexpr (!is_required<ValueType>()) {
        if (!_w.is_empty(&value)) {
          _w.set_field(name, value, _ptr);
        }
      } else {
        _w.set_field(name, value, _ptr);
      }
      return build_object_recursively<_i + 1>(_w, _tup, _ptr);
    }
  }

  /// Retrieves the value from the object. This is mainly needed to generate a
  /// better error message.
  template <class ValueType>
  static auto get_value(const ReaderType& _r,
                        std::pair<std::string, InputVarType> _pair) noexcept {
    const auto embellish_error = [&](const fct::Error& _e) {
      return fct::Error("Failed to parse field '" + _pair.first +
                        "': " + _e.what());
    };
    return Parser<ReaderType, WriterType, ValueType>::from_json(_r,
                                                                &_pair.second)
        .or_else(embellish_error);
  }
};

// ----------------------------------------------------------------------------

template <class ReaderType, class WriterType, class T>
struct Parser<ReaderType, WriterType, std::optional<T>> {
  using InputVarType = typename ReaderType::InputVarType;
  using OutputVarType = typename WriterType::OutputVarType;

  /// Expresses the variables as type T.
  static fct::Result<std::optional<T>> from_json(const ReaderType& _r,
                                                 InputVarType* _var) noexcept {
    if (_r.is_empty(_var)) {
      return std::optional<T>();
    }
    const auto to_opt = [&_r](auto&& _t) { return std::make_optional<T>(_t); };
    return Parser<ReaderType, WriterType, std::decay_t<T>>::from_json(_r, _var)
        .transform(to_opt);
  }

  /// Expresses the variable a a JSON.
  static OutputVarType to_json(const WriterType& _w,
                               const std::optional<T>& _o) noexcept {
    if (!_o) {
      return _w.empty_var();
    }
    return Parser<ReaderType, WriterType, std::decay_t<T>>::to_json(_w, *_o);
  }
};

// ----------------------------------------------------------------------------

template <class ReaderType, class WriterType, class T>
struct Parser<ReaderType, WriterType, fct::Ref<T>> {
  using InputVarType = typename ReaderType::InputVarType;
  using OutputVarType = typename WriterType::OutputVarType;

  /// Expresses the variables as type T.
  static fct::Result<fct::Ref<T>> from_json(const ReaderType& _r,
                                            InputVarType* _var) noexcept {
    const auto to_ref = [&](auto&& _t) { return fct::Ref<T>::make(_t); };
    return Parser<ReaderType, WriterType, std::decay_t<T>>::from_json(_r, _var)
        .transform(to_ref);
  }

  /// Expresses the variable a a JSON.
  static OutputVarType to_json(const WriterType& _w,
                               const fct::Ref<T>& _ref) noexcept {
    return Parser<ReaderType, WriterType, std::decay_t<T>>::to_json(_w, *_ref);
  }
};

// ----------------------------------------------------------------------------

template <class ReaderType, class WriterType, class T>
struct Parser<ReaderType, WriterType, std::shared_ptr<T>> {
  using InputVarType = typename ReaderType::InputVarType;
  using OutputVarType = typename WriterType::OutputVarType;

  /// Expresses the variables as type T.
  static fct::Result<std::shared_ptr<T>> from_json(
      const ReaderType& _r, InputVarType* _var) noexcept {
    if (_r.is_empty(_var)) {
      return std::shared_ptr<T>();
    }
    const auto to_ptr = [](auto&& _t) { return std::make_shared<T>(_t); };
    return Parser<ReaderType, WriterType, std::decay_t<T>>::from_json(_r, _var)
        .transform(to_ptr);
  }

  /// Expresses the variable a a JSON.
  static OutputVarType to_json(const WriterType& _w,
                               const std::shared_ptr<T>& _s) noexcept {
    if (!_s) {
      return _w.empty_var();
    }
    return Parser<ReaderType, WriterType, std::decay_t<T>>::to_json(_w, *_s);
  }
};

// ----------------------------------------------------------------------------

template <class ReaderType, class WriterType, class FirstType, class SecondType>
struct Parser<ReaderType, WriterType, std::pair<FirstType, SecondType>> {
  using InputVarType = typename ReaderType::InputVarType;
  using OutputVarType = typename WriterType::OutputVarType;

  /// Expresses the variables as type T.
  static fct::Result<std::pair<FirstType, SecondType>> from_json(
      const ReaderType& _r, InputVarType* _var) noexcept {
    const auto to_pair = [&](auto&& _t) {
      return std::make_pair(std::move(std::get<0>(_t)),
                            std::move(std::get<1>(_t)));
    };
    return Parser<ReaderType, WriterType,
                  std::tuple<FirstType, SecondType>>::from_json(_r, _var)
        .transform(to_pair);
  }

  /// Transform a std::vector into an array
  static OutputVarType to_json(
      const WriterType& _w,
      const std::pair<FirstType, SecondType>& _p) noexcept {
    const auto tup = std::make_tuple(_p.first, _p.second);
    return Parser<ReaderType, WriterType,
                  std::tuple<FirstType, SecondType>>::to_json(_w, tup);
  }
};

// ----------------------------------------------------------------------------

template <class ReaderType, class WriterType, class T>
struct Parser<ReaderType, WriterType, std::set<T>> {
  using InputArrayType = typename ReaderType::InputArrayType;
  using InputVarType = typename ReaderType::InputVarType;

  using OutputArrayType = typename WriterType::OutputArrayType;
  using OutputVarType = typename WriterType::OutputVarType;

 public:
  /// Expresses the variables as type T.
  static fct::Result<std::set<T>> from_json(const ReaderType& _r,
                                            InputVarType* _var) noexcept {
    const auto get_value = [&_r](InputVarType& _var) {
      return Parser<ReaderType, WriterType, std::decay_t<T>>::from_json(_r,
                                                                        &_var);
    };

    const auto to_set = [&_r, get_value](InputArrayType _arr) {
      auto vec = _r.to_vec(&_arr);
      return fct::collect_results::set(vec | VIEWS::transform(get_value));
    };

    return _r.to_array(_var).and_then(to_set);
  }

  /// Transform a std::vector into an array
  static OutputVarType to_json(const WriterType& _w,
                               const std::set<T>& _s) noexcept {
    auto arr = _w.new_array();
    for (const auto& val : _s) {
      _w.add(Parser<ReaderType, WriterType, std::decay_t<T>>::to_json(_w, val),
             &arr);
    }
    return arr;
  }
};

// ----------------------------------------------------------------------------

template <class ReaderType, class WriterType>
struct Parser<ReaderType, WriterType, strings::String> {
  using InputVarType = typename ReaderType::InputVarType;
  using OutputVarType = typename WriterType::OutputVarType;

  static fct::Result<strings::String> from_json(const ReaderType& _r,
                                                InputVarType* _var) noexcept {
    const auto to_str = [](auto&& _str) { return strings::String(_str); };
    return Parser<ReaderType, WriterType, std::string>::from_json(_r, _var)
        .transform(to_str);
  }

  static OutputVarType to_json(const WriterType& _w,
                               const strings::String& _s) noexcept {
    return Parser<ReaderType, WriterType, std::string>::to_json(_w, _s.str());
  }
};

// ----------------------------------------------------------------------------

template <class ReaderType, class WriterType, fct::StringLiteral _discriminator,
          class... NamedTupleTypes>
struct Parser<ReaderType, WriterType,
              fct::TaggedUnion<_discriminator, NamedTupleTypes...>> {
  using ResultType =
      fct::Result<fct::TaggedUnion<_discriminator, NamedTupleTypes...>>;

 public:
  using InputObjectType = typename ReaderType::InputObjectType;
  using InputVarType = typename ReaderType::InputVarType;

  using OutputObjectType = typename WriterType::OutputObjectType;
  using OutputVarType = typename WriterType::OutputVarType;

  /// Expresses the variables as type T.
  static ResultType from_json(const ReaderType& _r,
                              InputVarType* _var) noexcept {
    const auto get_disc = [&_r](auto _obj) {
      return get_discriminator(_r, &_obj);
    };

    const auto to_result = [&_r, _var](std::string _disc_value) {
      return find_matching_named_tuple(_r, _disc_value, _var);
    };

    return _r.to_object(_var).and_then(get_disc).and_then(to_result);
  }

  /// Expresses the variables as a JSON type.
  static OutputVarType to_json(
      const WriterType& _w,
      const fct::TaggedUnion<_discriminator, NamedTupleTypes...>&
          _tagged_union) noexcept {
    using VariantType =
        typename fct::TaggedUnion<_discriminator,
                                  NamedTupleTypes...>::VariantType;
    return Parser<ReaderType, WriterType, VariantType>::to_json(
        _w, _tagged_union.variant_);
  }

 private:
  template <int _i = 0>
  static ResultType find_matching_named_tuple(const ReaderType& _r,
                                              const std::string& _disc_value,
                                              InputVarType* _var) noexcept {
    if constexpr (_i == sizeof...(NamedTupleTypes)) {
      return fct::Error("Could not parse tagged union, could not match " +
                        _discriminator.str() + " '" + _disc_value + "'.");
    } else {
      const auto optional = try_option<_i>(_r, _disc_value, _var);
      if (optional) {
        return *optional;
      } else {
        return find_matching_named_tuple<_i + 1>(_r, _disc_value, _var);
      }
    }
  }

  /// Retrieves the discriminator.
  static fct::Result<std::string> get_discriminator(
      const ReaderType& _r, InputObjectType* _obj) noexcept {
    const auto embellish_error = [](const auto& _err) {
      return fct::Error("Could not parse tagged union: Could not find field '" +
                        _discriminator.str() +
                        "' or type of field was not a string.");
    };

    const auto to_type = [&_r](auto _var) {
      return _r.template to_basic_type<std::string>(&_var);
    };

    return _r.get_field(_discriminator.str(), _obj)
        .and_then(to_type)
        .or_else(embellish_error);
  }

  /// Determines whether the discriminating literal contains the value
  /// retrieved from the object.
  template <class T>
  static inline bool contains_disc_value(
      const std::string& _disc_value) noexcept {
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
  static std::optional<ResultType> try_option(const ReaderType& _r,
                                              const std::string& _disc_value,
                                              InputVarType* _var) noexcept {
    using NamedTupleType = std::decay_t<
        std::variant_alternative_t<_i, std::variant<NamedTupleTypes...>>>;

    if (contains_disc_value<NamedTupleType>(_disc_value)) {
      const auto to_tagged_union = [](const auto& _val) {
        return fct::TaggedUnion<_discriminator, NamedTupleTypes...>(_val);
      };

      const auto embellish_error = [&](const fct::Error& _e) {
        return fct::Error("Could not parse tagged union with discrimininator " +
                          _discriminator.str() + " '" + _disc_value +
                          "': " + _e.what());
      };

      return Parser<ReaderType, WriterType, NamedTupleType>::from_json(_r, _var)
          .transform(to_tagged_union)
          .or_else(embellish_error);

    } else {
      return std::optional<ResultType>();
    }
  }
};

// ----------------------------------------------------------------------------

template <class ReaderType, class WriterType, class... Ts>
struct Parser<ReaderType, WriterType, std::tuple<Ts...>> {
 public:
  using InputArrayType = typename ReaderType::InputArrayType;
  using InputVarType = typename ReaderType::InputVarType;

  using OutputArrayType = typename WriterType::OutputArrayType;
  using OutputVarType = typename WriterType::OutputVarType;

  /// Expresses the variables as type T.
  static fct::Result<std::tuple<Ts...>> from_json(const ReaderType& _r,
                                                  InputVarType* _var) noexcept {
    const auto to_vec = [&](auto _arr) { return _r.to_vec(&_arr); };

    const auto check_size =
        [](auto _vec) -> fct::Result<std::vector<InputVarType>> {
      if (_vec.size() != sizeof...(Ts)) {
        return fct::Error("Expected " + std::to_string(sizeof...(Ts)) +
                          " fields, got " + std::to_string(_vec.size()) + ".");
      }
      return std::move(_vec);
    };

    const auto extract = [&_r](auto _vec) {
      return extract_field_by_field(_r, std::move(_vec));
    };

    return _r.to_array(_var)
        .transform(to_vec)
        .and_then(check_size)
        .and_then(extract);
  }

  /// Transform a std::vector into a array
  static OutputVarType to_json(const WriterType& _w,
                               const std::tuple<Ts...>& _tup) noexcept {
    auto arr = _w.new_array();
    to_array<0>(_w, _tup, &arr);
    return arr;
  }

 private:
  /// Extracts values from the array, field by field.
  template <class... AlreadyExtracted>
  static fct::Result<std::tuple<Ts...>> extract_field_by_field(
      const ReaderType& _r, std::vector<InputVarType> _vec,
      const AlreadyExtracted&... _already_extracted) noexcept {
    constexpr size_t i = sizeof...(AlreadyExtracted);
    if constexpr (i == sizeof...(Ts)) {
      return std::tuple<Ts...>(std::make_tuple(_already_extracted...));
    } else {
      const auto extract_next = [&](auto new_entry) {
        return extract_field_by_field(_r, std::move(_vec),
                                      _already_extracted..., new_entry);
      };
      return extract_single_field<i>(_r, &_vec).and_then(extract_next);
    }
  }

  /// Extracts a single field from a JSON.
  template <int _i>
  static auto extract_single_field(const ReaderType& _r,
                                   std::vector<InputVarType>* _vec) noexcept {
    using NewFieldType =
        std::decay_t<typename std::tuple_element<_i, std::tuple<Ts...>>::type>;
    return Parser<ReaderType, WriterType, NewFieldType>::from_json(
        _r, &((*_vec)[_i]));
  }

  /// Transforms a tuple to an array.
  template <int _i>
  static void to_array(const WriterType& _w, const std::tuple<Ts...>& _tup,
                       OutputArrayType* _ptr) noexcept {
    if constexpr (_i < sizeof...(Ts)) {
      using NewFieldType = std::decay_t<
          typename std::tuple_element<_i, std::tuple<Ts...>>::type>;

      const auto val = Parser<ReaderType, WriterType, NewFieldType>::to_json(
          _w, std::get<_i>(_tup));
      _w.add(val, _ptr);
      to_array<_i + 1>(_w, _tup, _ptr);
    }
  }
};

// ----------------------------------------------------------------------------

template <class ReaderType, class WriterType, class... FieldTypes>
struct Parser<ReaderType, WriterType, std::variant<FieldTypes...>> {
  using InputVarType = typename ReaderType::InputVarType;
  using OutputVarType = typename WriterType::OutputVarType;

  /// Expresses the variables as type T.
  template <int _i = 0>
  static fct::Result<std::variant<FieldTypes...>> from_json(
      const ReaderType& _r, InputVarType* _var,
      const std::vector<std::string> _errors = {}) noexcept {
    if constexpr (_i == sizeof...(FieldTypes)) {
      return fct::Error("Could not parse variant: " +
                        fct::collect::string(_errors));
    } else {
      const auto to_variant = [](const auto& _val) {
        return std::variant<FieldTypes...>(_val);
      };

      const auto try_next = [&_r, _var, &_errors](const auto& _err) {
        const auto errors = fct::join::vector<std::string>(
            {_errors,
             std::vector<std::string>({std::string("\n -") + _err.what()})});
        return from_json<_i + 1>(_r, _var, errors);
      };

      using AltType = std::decay_t<
          std::variant_alternative_t<_i, std::variant<FieldTypes...>>>;

      return Parser<ReaderType, WriterType, AltType>::from_json(_r, _var)
          .transform(to_variant)
          .or_else(try_next);
    }
  }

  /// Expresses the variables as a JSON type.
  template <int _i = 0>
  static OutputVarType to_json(
      const WriterType& _w,
      const std::variant<FieldTypes...>& _variant) noexcept {
    using AltType = std::variant_alternative_t<_i, std::variant<FieldTypes...>>;
    if constexpr (_i + 1 == sizeof...(FieldTypes)) {
      return Parser<ReaderType, WriterType, std::decay_t<AltType>>::to_json(
          _w, std::get<AltType>(_variant));
    } else {
      if (std::holds_alternative<AltType>(_variant)) {
        return Parser<ReaderType, WriterType, std::decay_t<AltType>>::to_json(
            _w, std::get<AltType>(_variant));
      } else {
        return to_json<_i + 1>(_w, _variant);
      }
    }
  }
};

// ----------------------------------------------------------------------------

template <class ReaderType, class WriterType, class T>
struct Parser<ReaderType, WriterType, std::vector<T>> {
 public:
  using InputArrayType = typename ReaderType::InputArrayType;
  using InputVarType = typename ReaderType::InputVarType;

  using OutputArrayType = typename WriterType::OutputArrayType;
  using OutputVarType = typename WriterType::OutputVarType;

  /// Expresses the variables as type T.
  static fct::Result<std::vector<T>> from_json(const ReaderType& _r,
                                               InputVarType* _var) noexcept {
    const auto get_value = [&_r](InputVarType& _var) {
      return Parser<ReaderType, WriterType, std::decay_t<T>>::from_json(_r,
                                                                        &_var);
    };

    const auto to_vec = [&_r, get_value](InputArrayType _arr) {
      auto vec = _r.to_vec(&_arr);
      return fct::collect_results::vector(vec | VIEWS::transform(get_value));
    };

    return _r.to_array(_var).and_then(to_vec);
  }

  /// Transform a std::vector into an array
  static OutputVarType to_json(const WriterType& _w,
                               const std::vector<T>& _vec) noexcept {
    auto arr = _w.new_array();
    for (size_t i = 0; i < _vec.size(); ++i) {
      _w.add(
          Parser<ReaderType, WriterType, std::decay_t<T>>::to_json(_w, _vec[i]),
          &arr);
    }
    return OutputVarType(arr);
  }
};

}  // namespace parsing

#endif  // JSON_PARSER_HPP_
