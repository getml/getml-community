// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_NOTSUPPORTEDINCOMMUNITY_HPP_
#define COMMANDS_NOTSUPPORTEDINCOMMUNITY_HPP_

#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/internal/StringLiteral.hpp"

namespace commands {

template <rfl::internal::StringLiteral type_>
using NotSupportedInCommunity =
    rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<type_>>>;

}  // namespace commands

#endif
