// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_FINGERPRINT_HPP_
#define COMMANDS_FINGERPRINT_HPP_

#include <variant>

#include "commands/DataFrameOrView.hpp"
#include "commands/FeatureLearner.hpp"
#include "commands/Preprocessor.hpp"
#include "predictors/PredictorHyperparams.hpp"

namespace commands {

/// Fingerprints are used to track the dirty states of a pipeline (which
/// prevents the user from fitting the same thing over and over again).
using Fingerprint = std::variant<DataFrameOrView, Preprocessor, FeatureLearner,
                                 predictors::PredictorHyperparams>;

}  // namespace commands

#endif  // COMMANDS_FINGERPRINT_HPP_

