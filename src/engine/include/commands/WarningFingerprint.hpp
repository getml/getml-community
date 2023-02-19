// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_WARNINGFINGERPRINT_HPP_
#define COMMANDS_WARNINGFINGERPRINT_HPP_

#include <cstddef>
#include <string>
#include <vector>

#include "commands/PredictorFingerprint.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"

namespace commands {

/// To avoid duplicate checks, we also include the warnings into the fingerprint
/// system.
using WarningFingerprint = fct::NamedTuple<
    fct::Field<"fl_fingerprints_",
               std::vector<typename PredictorFingerprint::DependencyType>>>;

}  // namespace commands

#endif  // COMMANDS_WARNINGFINGERPRINT_HPP_
