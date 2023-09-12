// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_INTERNAL_ISBASEFIELD_HPP_
#define RFL_INTERNAL_ISBASEFIELD_HPP_

#include <tuple>
#include <type_traits>
#include <utility>

#include "rfl/Base.hpp"

namespace rfl {
namespace internal {

template <class T>
class is_base_field;

template <class T>
class is_base_field : public std::false_type {};

template <class T>
class is_base_field<Base<T>> : public std::true_type {};

}  // namespace internal
}  // namespace rfl

#endif
