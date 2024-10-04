// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_PREDICTORPARSER_HPP_
#define PREDICTORS_PREDICTORPARSER_HPP_

#include <rfl/Ref.hpp>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "predictors/Predictor.hpp"
#include "predictors/PredictorHyperparams.hpp"
#include "predictors/PredictorImpl.hpp"

namespace predictors {

struct PredictorParser {
  /// Parses the predictor.
  static rfl::Ref<Predictor> parse(
      const PredictorHyperparams& _cmd,
      const rfl::Ref<const PredictorImpl>& _impl,
      const std::vector<commands::Fingerprint>& _dependencies);
};

}  // namespace predictors

#endif  // PREDICTORS_PREDICTORPARSER_HPP_
