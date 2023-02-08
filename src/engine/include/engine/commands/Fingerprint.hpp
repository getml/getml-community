// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_COMMANDS_FINGERPRINT_HPP_
#define ENGINE_COMMANDS_FINGERPRINT_HPP_

#include <variant>

#include "engine/commands/DataFrameOrView.hpp"
#include "engine/commands/FeatureLearner.hpp"
#include "engine/commands/Preprocessor.hpp"
#include "predictors/PredictorHyperparams.hpp"

namespace engine {
namespace commands {

/// Fingerprints are used to track the dirty states of a pipeline (which
/// prevents the user from fitting the same thing over and over again).
using Fingerprint = std::variant<DataFrameOrView, Preprocessor, FeatureLearner,
                                 predictors::PredictorHyperparams>;

}  // namespace commands
}  // namespace engine

#endif  // ENGINE_COMMANDS_FINGERPRINT_HPP_

