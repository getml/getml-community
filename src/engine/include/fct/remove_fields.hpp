// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_REMOVEFIELDS_HPP_
#define FCT_REMOVEFIELDS_HPP_

#include <algorithm>
#include <tuple>
#include <type_traits>

#include "fct/NamedTuple.hpp"
#include "fct/StringLiteral.hpp"
#include "fct/define_named_tuple.hpp"

namespace fct {

/// Recursively builds a new NamedTuple type from the FieldTypes, leaving out
/// the field signified by _name.
template <class _OldNamedTupleType, StringLiteral _name,
          class _NewNamedTupleType, int _i>
struct remove_single_field;

/// Special case - _i == 0
template <class _OldNamedTupleType, StringLiteral _name,
          class _NewNamedTupleType>
struct remove_single_field<_OldNamedTupleType, _name, _NewNamedTupleType, 0> {
  using type = _NewNamedTupleType;
};

/// General case.
template <class _OldNamedTupleType, StringLiteral _name,
          class _NewNamedTupleType, int _i>
struct remove_single_field {
  using OldNamedTupleType = std::decay_t<_OldNamedTupleType>;

  constexpr static int num_fields =
      std::tuple_size<typename OldNamedTupleType::Fields>{};

  using FieldType = std::decay_t<typename std::tuple_element<
      num_fields - _i, typename OldNamedTupleType::Fields>::type>;

  using NewNamedTupleType =
      std::conditional_t<_name == FieldType::name_, _NewNamedTupleType,
                         define_named_tuple_t<_NewNamedTupleType, FieldType>>;

  using type = typename remove_single_field<OldNamedTupleType, _name,
                                            NewNamedTupleType, _i - 1>::type;
};

/// Recursively removes all of the fields signified by _head and _tail from the
/// NamedTupleType.
template <class _NamedTupleType, StringLiteral _head, StringLiteral... _tail>
struct remove_fields;

/// Special case - only head is left.
template <class _NamedTupleType, StringLiteral _head>
struct remove_fields<_NamedTupleType, _head> {
  using NamedTupleType = std::decay_t<_NamedTupleType>;

  constexpr static int num_fields =
      std::tuple_size<typename NamedTupleType::Fields>{};

  using type = typename remove_single_field<NamedTupleType, _head, NamedTuple<>,
                                            num_fields>::type;
};

/// General case.
template <class _NamedTupleType, StringLiteral _head, StringLiteral... _tail>
struct remove_fields {
  using NamedTupleType = std::decay_t<_NamedTupleType>;

  constexpr static int num_fields =
      std::tuple_size<typename NamedTupleType::Fields>{};

  using NewNamedTupleType =
      typename remove_single_field<NamedTupleType, _head, NamedTuple<>,
                                   num_fields>::type;

  using type = typename remove_fields<NewNamedTupleType, _tail...>::type;
};

/// Recursively removes all of the fields signified by _names from the
/// NamedTupleType.
template <class NamedTupleType, StringLiteral... _names>
using remove_fields_t =
    typename remove_fields<std::decay_t<NamedTupleType>, _names...>::type;

}  // namespace fct

#endif  // FCT_REMOVEFIELDS_HPP_
