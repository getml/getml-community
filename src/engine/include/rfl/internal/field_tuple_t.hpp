// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_INTERNAL_FIELD_TUPLE_T_HPP_
#define RFL_INTERNAL_FIELD_TUPLE_T_HPP_

#include <functional>
#include <tuple>
#include <type_traits>

#include "rfl/internal/to_field_tuple.hpp"

namespace rfl {
namespace internal {

template <class T>
using field_tuple_t =
    typename std::invoke_result<decltype(to_field_tuple<T>), T>::type;

}
}  // namespace rfl

#endif
