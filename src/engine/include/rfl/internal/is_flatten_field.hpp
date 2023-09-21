// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_INTERNAL_ISFLATTENFIELD_HPP_
#define RFL_INTERNAL_ISFLATTENFIELD_HPP_

#include <tuple>
#include <type_traits>
#include <utility>

#include "rfl/Flatten.hpp"

namespace rfl {
namespace internal {

template <class T>
class is_flatten_field;

template <class T>
class is_flatten_field : public std::false_type {};

template <class T>
class is_flatten_field<Flatten<T>> : public std::true_type {};

}  // namespace internal
}  // namespace rfl

#endif
