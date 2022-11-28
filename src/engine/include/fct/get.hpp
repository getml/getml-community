// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_GET_HPP_
#define FCT_GET_HPP_

#include <tuple>

#include "fct/StringLiteral.hpp"
#include "fct/find_index.hpp"

namespace fct {

/// Gets a field by index.
template <int _index, class NamedTupleType>
inline const auto& get(const NamedTupleType& _tup) {
  return std::get<_index>(_tup.values());
}

/// Gets a field by name.
template <StringLiteral _field_name, class NamedTupleType>
inline const auto& get(const NamedTupleType& _tup) {
  constexpr auto index =
      find_index<_field_name, typename NamedTupleType::Fields>();
  return std::get<index>(_tup.values());
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
  return std::get<index>(_tup.values());
}

}  // namespace fct

#endif
