// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_INTERNAL_IS_FIELD_HPP_
#define RFL_INTERNAL_IS_FIELD_HPP_

#include <type_traits>

#include "rfl/Field.hpp"

namespace rfl {
namespace internal {

template <class T>
struct IsFieldType;

template <class T>
struct IsFieldType {
  constexpr static bool is_field_ = false;
  using Type = std::decay_t<T>;
};

template <StringLiteral _name, class _Type>
struct IsFieldType<Field<_name, _Type>> {
  constexpr static bool is_field_ = true;
  using Type = std::decay_t<_Type>;
};

template <class T>
using is_field_v = typename IsFieldType<T>::is_field_;

template <class T>
using is_field_t = typename IsFieldType<T>::Type_;

}  // namespace internal
}  // namespace rfl

#endif
