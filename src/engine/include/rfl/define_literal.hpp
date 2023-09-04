// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_DEFINELITERAL_HPP_
#define RFL_DEFINELITERAL_HPP_

#include "rfl/Literal.hpp"
#include "rfl/internal/define_literal.hpp"

namespace rfl {

/// Allows you to combine several literal types.
template <class... LiteralTypes>
using define_literal_t =
    typename internal::define_literal<LiteralTypes...>::type;

}  // namespace rfl

#endif  // RFL_DEFINELITERAL_HPP_
