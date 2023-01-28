// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_FINGERPRINTS_HPP_
#define ENGINE_COMMANDS_FINGERPRINTS_HPP_

#include <variant>
#include <vector>

#include "engine/commands/DataFrameOrView.hpp"
#include "engine/commands/Preprocessor.hpp"

namespace engine {
namespace commands {

/// Fingerprints are used to track the dirty states of a pipeline (which
/// prevents the user from fitting the same thing over and over again).

using Fingerprints =
    std::variant<std::vector<DataFrameOrView>, std::vector<Preprocessor>>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_FINGERPRINTS_HPP_

