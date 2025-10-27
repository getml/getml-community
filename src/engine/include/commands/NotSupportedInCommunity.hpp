// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_NOTSUPPORTEDINCOMMUNITY_HPP_
#define COMMANDS_NOTSUPPORTEDINCOMMUNITY_HPP_

#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/internal/StringLiteral.hpp>

namespace commands {

template <rfl::internal::StringLiteral name_>
struct NotSupportedInCommunity {
  using Tag = rfl::Literal<name_>;

  std::string name() const { return Tag().str(); }
};

}  // namespace commands

#endif
