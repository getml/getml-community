// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_PREDICTORPARSER_HPP_
#define PREDICTORS_PREDICTORPARSER_HPP_

#include <memory>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "fct/Ref.hpp"
#include "json/json.hpp"
#include "predictors/Predictor.hpp"
#include "predictors/PredictorHyperparams.hpp"
#include "predictors/PredictorImpl.hpp"

namespace predictors {

struct PredictorParser {
  /// Parses the predictor.
  static fct::Ref<Predictor> parse(
      const PredictorHyperparams& _cmd,
      const fct::Ref<const PredictorImpl>& _impl,
      const std::vector<commands::Fingerprint>& _dependencies);
};

}  // namespace predictors

#endif  // PREDICTORS_PREDICTORPARSER_HPP_
