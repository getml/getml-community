// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_NAMEDTUPLE_HPP_
#define FCT_NAMEDTUPLE_HPP_

#include <algorithm>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "fct/Field.hpp"
#include "fct/StringLiteral.hpp"
#include "fct/find_index.hpp"
#include "fct/get.hpp"
#include "fct/has_named_tuple_method_v.hpp"
#include "fct/has_named_tuple_type_v.hpp"

namespace fct {

/// A named tuple behaves like std::tuple,
/// but the fields have explicit names, which
/// allows for reflection.
template <class... FieldTypes>
class NamedTuple {
 public:
  using Fields = std::tuple<std::decay_t<FieldTypes>...>;

 public:
  /// Construct from the fields.
  NamedTuple(FieldTypes... _fields)
      : values_(std::forward_as_tuple(_fields.value_...)) {
    static_assert(no_duplicate_field_names(),
                  "Duplicate field names are not allowed");
  }

  /// Construct from a tuple containing fields.
  NamedTuple(const std::tuple<FieldTypes...>& _tup)
      : NamedTuple(std::make_from_tuple<NamedTuple<FieldTypes...>>(_tup)) {
    static_assert(no_duplicate_field_names(),
                  "Duplicate field names are not allowed");
  }

  /// Copy constructor.
  NamedTuple(const NamedTuple<FieldTypes...>& _other)
      : values_(_other.values_) {
    static_assert(no_duplicate_field_names(),
                  "Duplicate field names are not allowed");
  }

  /// Move constructor.
  NamedTuple(NamedTuple<FieldTypes...>&& _other)
      : values_(std::move(_other.values_)) {
    static_assert(no_duplicate_field_names(),
                  "Duplicate field names are not allowed");
  }

  /// Copy constructor.
  template <class... OtherFieldTypes>
  NamedTuple(const NamedTuple<OtherFieldTypes...>& _other)
      : NamedTuple(retrieve_fields(_other.fields())) {
    static_assert(no_duplicate_field_names(),
                  "Duplicate field names are not allowed");
  }

  /// Move constructor.
  template <class... OtherFieldTypes>
  NamedTuple(NamedTuple<OtherFieldTypes...>&& _other)
      : NamedTuple(retrieve_fields(_other.fields())) {
    static_assert(no_duplicate_field_names(),
                  "Duplicate field names are not allowed");
  }

  ~NamedTuple() = default;

  /// Returns a new named tuple with additional fields.
  template <class Head, class... Tail>
  auto add(Head&& _head, Tail&&... _tail) const {
    return NamedTuple<FieldTypes..., std::decay_t<Head>, Tail...>(
        make_fields<sizeof...(Tail) + 1, Head, Tail...>(
            std::forward<Head>(_head), std::forward<Tail>(_tail)...));
  }

  /// Template specialization for std::tuple, so we can pass fields from other
  /// named tuples.
  template <class... TupContent, class... Tail>
  auto add(std::tuple<TupContent...>&& _tuple, Tail&&... _tail) const {
    return add_tuple(std::forward<std::tuple<TupContent...>>(_tuple),
                     std::forward<Tail>(_tail)...,
                     std::make_index_sequence<sizeof...(TupContent)>{});
  }

  /// Template specialization for NamedTuple, so we can pass fields from other
  /// named tuples.
  template <class... TupContent, class... Tail>
  auto add(NamedTuple<TupContent...>&& _named_tuple, Tail&&... _tail) const {
    return add(_named_tuple.fields(), std::forward<Tail>(_tail)...);
  }

  /// Returns a tuple containing the fields.
  Fields fields() const { return make_fields(); }

  /// Gets a field by index.
  template <int _index>
  inline auto& get() {
    return fct::get<_index>(*this);
  }

  /// Gets a field by name.
  template <StringLiteral _field_name>
  inline auto& get() {
    return fct::get<_field_name>(*this);
  }

  /// Gets a field by the field type.
  template <class Field>
  inline auto& get() {
    return fct::get<Field>(*this);
  }

  /// Gets a field by index.
  template <int _index>
  inline const auto& get() const {
    return fct::get<_index>(*this);
  }

  /// Gets a field by name.
  template <StringLiteral _field_name>
  inline const auto& get() const {
    return fct::get<_field_name>(*this);
  }

  /// Gets a field by the field type.
  template <class Field>
  inline const auto& get() const {
    return fct::get<Field>(*this);
  }

  /// Copy assignment operator.
  NamedTuple<FieldTypes...>& operator=(
      const NamedTuple<FieldTypes...>& _other) {
    values_ = _other.values_;
    return *this;
  }

  /// Move assignment operator.
  NamedTuple<FieldTypes...>& operator=(
      NamedTuple<FieldTypes...>&& _other) noexcept {
    if (this == &_other) {
      return *this;
    }
    values_ = std::move(_other.values_);
    return *this;
  }

  /// Replaces one or several fields, returning a new version
  /// with the non-replaced fields left unchanged.
  template <class RField, class... OtherRFields>
  NamedTuple<FieldTypes...> replace(RField&& _field,
                                    OtherRFields&&... _other_fields) const {
    constexpr auto num_other_fields = sizeof...(OtherRFields);
    if constexpr (num_other_fields == 0) {
      return replace_value<RField>(_field.value_);
    } else {
      return replace_value<RField>(_field.value_)
          .replace(std::forward<OtherRFields>(_other_fields)...);
    }
  }

  /// Template specialization for std::tuple, so we can pass fields from other
  /// named tuples.
  template <class... TupContent, class... Tail>
  auto replace(std::tuple<TupContent...>&& _tuple, Tail&&... _tail) const {
    return replace_tuple(std::forward<std::tuple<TupContent...>>(_tuple),
                         std::forward<Tail>(_tail)...,
                         std::make_index_sequence<sizeof...(TupContent)>{});
  }

  /// Template specialization for NamedTuple, so we can pass fields from other
  /// named tuples.
  template <class... TupContent, class... Tail>
  auto replace(NamedTuple<TupContent...>&& _named_tuple,
               Tail&&... _tail) const {
    return replace(_named_tuple.fields(), std::forward<Tail>(_tail)...);
  }

  /// Returns the underlying std::tuple.
  auto& values() { return values_; }

  /// Returns the underlying std::tuple.
  const auto& values() const { return values_; }

 private:
  /// Adds the elements of a tuple to a newly created named tuple,
  /// and other elements to a newly created named tuple.
  template <class Tuple, class... Tail, size_t... Is>
  constexpr auto add_tuple(const Tuple& _tuple, const Tail&... _tail,
                           const std::index_sequence<Is...>) const {
    return add(std::get<Is>(_tuple)..., _tail...);
  }

  /// Generates the fields.
  template <int num_additional_fields = 0, class... Args>
  auto make_fields(Args&&... _args) const {
    constexpr auto size = sizeof...(Args) - num_additional_fields;
    constexpr auto num_fields = std::tuple_size<Fields>{};
    constexpr auto i = num_fields - size - 1;

    constexpr bool retrieved_all_fields = size == num_fields;

    if constexpr (retrieved_all_fields) {
      return std::make_tuple(std::forward<Args>(_args)...);
    } else {
      // When we add additional fields, it is more intuitive to add
      // them to the end, that is why we do it like this.
      using FieldType = typename std::tuple_element<i, Fields>::type;
      return make_fields<num_additional_fields>(FieldType(std::get<i>(values_)),
                                                std::forward<Args>(_args)...);
    }
  }

  /// Generates a new named tuple with one value replaced with a new value.
  template <int _index, class T, class... Args>
  auto make_replaced(T&& _val, Args&&... _args) const {
    constexpr auto size = sizeof...(Args);

    constexpr bool retrieved_all_fields = size == std::tuple_size<Fields>{};

    if constexpr (retrieved_all_fields) {
      return NamedTuple<FieldTypes...>(std::forward<Args>(_args)...);
    } else {
      using FieldType = typename std::tuple_element<size, Fields>::type;

      if constexpr (size == _index) {
        return make_replaced<_index, T>(std::forward<T>(_val),
                                        std::forward<Args>(_args)...,
                                        FieldType(_val));
      } else {
        return make_replaced<_index, T>(std::forward<T>(_val),
                                        std::forward<Args>(_args)...,
                                        FieldType(std::get<size>(values_)));
      }
    }
  }

  /// We cannot allow duplicate field names.
  template <int _i = 1, int _j = 0>
  constexpr static bool no_duplicate_field_names() {
    constexpr auto num_fields = std::tuple_size<Fields>{};

    if constexpr (_i == num_fields) {
      return true;
    } else if constexpr (_j == -1) {
      return no_duplicate_field_names<_i + 1, _i>();
    } else {
      using FieldType1 =
          std::decay_t<typename std::tuple_element<_i, Fields>::type>;
      using FieldType2 =
          std::decay_t<typename std::tuple_element<_j, Fields>::type>;

      constexpr auto field_name_i = FieldType1::name_;
      constexpr auto field_name_j = FieldType2::name_;

      constexpr bool no_duplicate = (field_name_i != field_name_j);

      static_assert(no_duplicate, "Duplicate field names are not allowed");

      return no_duplicate && no_duplicate_field_names<_i, _j - 1>();
    }
  }

  /// Replaced the field signified by the field type.
  template <class Field, class T>
  NamedTuple<FieldTypes...> replace_value(T&& _val) const {
    constexpr auto index = find_index<Field::name_, Fields>();
    static_assert(
        std::is_same<typename std::tuple_element<index, Fields>::type::Type,
                     typename Field::Type>(),
        "If two fields have the same name, "
        "their type must be the same as "
        "well.");
    return make_replaced<index, T>(_val);
  }

  /// Adds the elements of a tuple to a newly created named tuple,
  /// and other elements to a newly created named tuple.
  template <class Tuple, class... Tail, size_t... Is>
  constexpr auto replace_tuple(Tuple&& _tuple, Tail&&... _tail,
                               const std::index_sequence<Is...>) const {
    return replace(std::get<Is>(_tuple)..., std::forward<Tail>(_tail)...);
  }

  /// Retrieves the fields from another tuple.
  template <class... OtherFieldTypes, class... Args>
  constexpr static Fields retrieve_fields(
      std::tuple<OtherFieldTypes...>&& _other_fields, Args&&... _args) {
    constexpr auto size = sizeof...(Args);

    constexpr bool retrieved_all_fields = size == std::tuple_size<Fields>{};

    if constexpr (retrieved_all_fields) {
      return std::make_tuple(std::forward<Args>(_args)...);
    } else {
      constexpr auto field_name = std::tuple_element<size, Fields>::type::name_;

      constexpr auto index =
          find_index<field_name, std::tuple<OtherFieldTypes...>>();

      using FieldType = typename std::tuple_element<size, Fields>::type;

      return retrieve_fields(
          std::forward<std::tuple<OtherFieldTypes...>>(_other_fields),
          std::forward<Args>(_args)...,
          FieldType(std::get<index>(_other_fields).value_));
    }
  }

 private:
  /// The values actually contained in the named tuple.
  /// As you can see, a NamedTuple is just a normal tuple under-the-hood,
  /// everything else is resolved at compile time. It should have no
  /// runtime overhead over a normal std::tuple.
  std::tuple<typename std::decay<FieldTypes>::type::Type...> values_;
};

// ----------------------------------------------------------------------------

template <class... FieldTypes>
inline bool operator==(const fct::NamedTuple<FieldTypes...>& _nt1,
                       const fct::NamedTuple<FieldTypes...>& _nt2) {
  return _nt1.values() == _nt2.values();
}

template <class... FieldTypes>
inline bool operator!=(const fct::NamedTuple<FieldTypes...>& _nt1,
                       const fct::NamedTuple<FieldTypes...>& _nt2) {
  return _nt1.values() != _nt2.values();
}

template <StringLiteral _name1, class Type1, StringLiteral _name2, class Type2>
inline auto operator*(const fct::Field<_name1, Type1>& _f1,
                      const fct::Field<_name2, Type2>& _f2) {
  return NamedTuple(_f1, _f2);
}

template <StringLiteral _name, class Type, class... FieldTypes>
inline auto operator*(const NamedTuple<FieldTypes...>& _tup,
                      const fct::Field<_name, Type>& _f) {
  return _tup.add(_f);
}

template <StringLiteral _name, class Type, class... FieldTypes>
inline auto operator*(const fct::Field<_name, Type>& _f,
                      const NamedTuple<FieldTypes...>& _tup) {
  return NamedTuple(_f).add(_tup);
}

template <class... FieldTypes1, class... FieldTypes2>
inline auto operator*(const NamedTuple<FieldTypes1...>& _tup1,
                      const NamedTuple<FieldTypes2...>& _tup2) {
  return _tup1.add(_tup2);
}

template <StringLiteral _name1, class Type1, StringLiteral _name2, class Type2>
inline auto operator*(fct::Field<_name1, Type1>&& _f1,
                      fct::Field<_name2, Type2>&& _f2) {
  return NamedTuple(std::forward<Field<_name1, Type1>>(_f1),
                    std::forward<Field<_name2, Type2>>(_f2));
}

template <StringLiteral _name, class Type, class... FieldTypes>
inline auto operator*(NamedTuple<FieldTypes...>&& _tup,
                      fct::Field<_name, Type>&& _f) {
  return _tup.add(std::forward<Field<_name, Type>>(_f));
}

template <StringLiteral _name, class Type, class... FieldTypes>
inline auto operator*(fct::Field<_name, Type>&& _f,
                      NamedTuple<FieldTypes...>&& _tup) {
  return NamedTuple(std::forward<Field<_name, Type>>(_f))
      .add(std::forward<NamedTuple<FieldTypes...>>(_tup));
}

template <class... FieldTypes1, class... FieldTypes2>
inline auto operator*(NamedTuple<FieldTypes1...>&& _tup1,
                      NamedTuple<FieldTypes2...>&& _tup2) {
  return _tup1.add(std::forward<NamedTuple<FieldTypes2...>>(_tup2));
}

}  // namespace fct

#endif  // FCT_NAMEDTUPLE_HPP_
