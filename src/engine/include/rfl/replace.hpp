// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_REPLACE_HPP_
#define RFL_REPLACE_HPP_

#include "rfl/from_named_tuple.hpp"
#include "rfl/internal/is_named_tuple.hpp"
#include "rfl/to_named_tuple.hpp"

namespace rfl {

/// Replaces one or several fields, returning a new version
/// with the non-replaced fields left unchanged.
template <class T, class RField, class... OtherRFields>
auto replace(const T& _t, RField&& _field, OtherRFields&&... _other_fields) {
  if constexpr (internal::is_named_tuple_v<T>) {
    return _t.replace(_field, _other_fields...);
  } else {
    return from_named_tuple<T>(
        to_named_tuple(_t).replace(_field, _other_fields...));
  }
}

}  // namespace rfl

#endif
