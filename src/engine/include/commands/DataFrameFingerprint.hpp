// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATAFRAMEFINGERPRINT_HPP_
#define COMMANDS_DATAFRAMEFINGERPRINT_HPP_

#include <variant>

#include "commands/DataFrameOrView.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "helpers/Placeholder.hpp"

namespace commands {

/// Fingerprints are used to track the dirty states of a pipeline (which
/// prevents the user from fitting the same thing over and over again).
using DataFrameFingerprint =
    std::variant<typename DataFrameOrView::ViewOp,
                 fct::NamedTuple<fct::Field<"name_", std::string>,
                                 fct::Field<"last_change_", std::string>>,
                 fct::Ref<const helpers::Placeholder>>;

}  // namespace commands

#endif  // COMMANDS_DATAFRAMEFINGERPRINT_HPP_

