// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "predictors/PredictorParser.hpp"

#include "predictors/LinearRegression.hpp"
#include "predictors/LogisticRegression.hpp"
#include "predictors/XGBoostPredictor.hpp"

namespace predictors {

fct::Ref<Predictor> PredictorParser::parse(
    const Poco::JSON::Object& _json_obj,
    const std::shared_ptr<const PredictorImpl>& _impl,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies) {
  const std::string type = "TODO";

  if (type == "LinearRegression") {
    return fct::Ref<LinearRegression>::make(_json_obj, _impl, _dependencies);
  }

  if (type == "LogisticRegression") {
    return fct::Ref<LogisticRegression>::make(_json_obj, _impl, _dependencies);
  }

  if (type == "XGBoostPredictor" || type == "XGBoostClassifier" ||
      type == "XGBoostRegressor") {
    return fct::Ref<XGBoostPredictor>::make(_json_obj, _impl, _dependencies);
  }

  throw std::runtime_error("Predictor of type '" + type + "' not known!");
}

}  // namespace predictors
