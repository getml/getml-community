// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_WARNINGFINGERPRINT_HPP_
#define COMMANDS_WARNINGFINGERPRINT_HPP_

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>
#include <vector>

#include "commands/Fingerprint.hpp"

namespace commands {

/// To avoid duplicate checks, we also include the warnings into the fingerprint
/// system.
using WarningFingerprint = rfl::NamedTuple<rfl::Field<
    "fl_fingerprints_", rfl::Ref<const std::vector<commands::Fingerprint>>>>;

}  // namespace commands

#endif  // COMMANDS_WARNINGFINGERPRINT_HPP_
