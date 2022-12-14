// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef PREDICTORS_PREDICTORPARSER_HPP_
#define PREDICTORS_PREDICTORPARSER_HPP_

// -----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// -----------------------------------------------------------------------------

#include <memory>
#include <vector>

// -----------------------------------------------------------------------------

#include "fct/Ref.hpp"

// -----------------------------------------------------------------------------

#include "predictors/Predictor.hpp"
#include "predictors/PredictorImpl.hpp"

// -----------------------------------------------------------------------------

namespace predictors {

struct PredictorParser {
  /// Parses the predictor.
  static fct::Ref<Predictor> parse(
      const Poco::JSON::Object& _json_obj,
      const std::shared_ptr<const PredictorImpl>& _impl,
      const std::vector<Poco::JSON::Object::Ptr>& _dependencies);
};

// ----------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_PREDICTORPARSER_HPP_
