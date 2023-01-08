// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_GET_HPP_
#define FCT_GET_HPP_

#include "fct/Getter.hpp"
#include "fct/StringLiteral.hpp"
#include "fct/find_index.hpp"

namespace fct {

/// Gets a field by index.
template <int _index, class NamedTupleType>
inline auto& get(NamedTupleType& _tup) {
  return Getter<NamedTupleType, _index>::get(_tup);
}

/// Gets a field by name.
template <StringLiteral _field_name, class NamedTupleType>
inline auto& get(NamedTupleType& _tup) {
  constexpr auto index =
      find_index<_field_name, typename NamedTupleType::Fields>();
  return Getter<NamedTupleType, index>::get(_tup);
}

/// Gets a field by the field type.
template <class Field, class NamedTupleType>
inline auto& get(NamedTupleType& _tup) {
  constexpr auto index =
      find_index<Field::name_, typename NamedTupleType::Fields>();
  static_assert(
      std::is_same<typename std::tuple_element<
                       index, typename NamedTupleType::Fields>::type::Type,
                   typename Field::Type>(),
      "If two fields have the same name, "
      "their type must be the same as "
      "well.");
  return Getter<NamedTupleType, index>::get(_tup);
}

/// Gets a field by index.
template <int _index, class NamedTupleType>
inline const auto& get(const NamedTupleType& _tup) {
  return Getter<NamedTupleType, _index>::get_const(_tup);
}

/// Gets a field by name.
template <StringLiteral _field_name, class NamedTupleType>
inline const auto& get(const NamedTupleType& _tup) {
  constexpr auto index =
      find_index<_field_name, typename NamedTupleType::Fields>();
  return Getter<NamedTupleType, index>::get_const(_tup);
}

/// Gets a field by the field type.
template <class Field, class NamedTupleType>
inline const auto& get(const NamedTupleType& _tup) {
  constexpr auto index =
      find_index<Field::name_, typename NamedTupleType::Fields>();
  static_assert(
      std::is_same<typename std::tuple_element<
                       index, typename NamedTupleType::Fields>::type::Type,
                   typename Field::Type>(),
      "If two fields have the same name, "
      "their type must be the same as "
      "well.");
  return Getter<NamedTupleType, index>::get_const(_tup);
}

}  // namespace fct

#endif
