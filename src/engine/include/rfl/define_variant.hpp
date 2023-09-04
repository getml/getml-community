// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_DEFINEVARIANT_HPP_
#define RFL_DEFINEVARIANT_HPP_

#include <variant>

#include "rfl/internal/define_variant.hpp"

namespace rfl {

template <class... Vars>
using define_variant_t = typename internal::define_variant<Vars...>::type;

}  // namespace rfl

#endif  // RFL_DEFINEVARIANT_HPP_
