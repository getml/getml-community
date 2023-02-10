// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_DATAFRAMEFINGERPRINT_HPP_
#define ENGINE_COMMANDS_DATAFRAMEFINGERPRINT_HPP_

#include <variant>

#include "engine/commands/DataFrameOrView.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"

namespace engine {
namespace commands {

/// Fingerprints are used to track the dirty states of a pipeline (which
/// prevents the user from fitting the same thing over and over again).
using DataFrameFingerprint =
    std::variant<typename DataFrameOrView::ViewOp,
                 fct::NamedTuple<fct::Field<"name_", std::string>,
                                 fct::Field<"last_change_", std::string>>>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_DATAFRAMEFINGERPRINT_HPP_

