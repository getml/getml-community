// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_NOTSUPPORTEDINCOMMUNITY_HPP_
#define COMMANDS_NOTSUPPORTEDINCOMMUNITY_HPP_

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/StringLiteral.hpp"

namespace commands {

template <fct::StringLiteral type_>
using NotSupportedInCommunity =
    fct::NamedTuple<fct::Field<"type_", fct::Literal<type_>>>;

}  // namespace commands

#endif
