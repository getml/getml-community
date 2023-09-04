// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_FIELD_TYPE_HPP_
#define RFL_FIELD_TYPE_HPP_

#include <tuple>
#include <type_traits>
#include <variant>

#include "rfl/internal/StringLiteral.hpp"
#include "rfl/internal/field_type.hpp"

namespace rfl {

template <internal::StringLiteral _field_name, class NamedTupleType>
using field_type_t =
    typename internal::FieldType<_field_name, NamedTupleType>::Type;

}  // namespace rfl

#endif
