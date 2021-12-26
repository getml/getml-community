#include "predictors/PredictorParser.hpp"

// ----------------------------------------------------------------------

#include "predictors/LinearRegression.hpp"
#include "predictors/LogisticRegression.hpp"
#include "predictors/XGBoostPredictor.hpp"

// ----------------------------------------------------------------------

namespace predictors {

std::shared_ptr<Predictor> PredictorParser::parse(
    const Poco::JSON::Object& _json_obj,
    const std::shared_ptr<const PredictorImpl>& _impl,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies) {
  const auto type = JSON::get_value<std::string>(_json_obj, "type_");

  if (type == "LinearRegression") {
    return std::make_shared<LinearRegression>(_json_obj, _impl, _dependencies);
  } else if (type == "LogisticRegression") {
    return std::make_shared<LogisticRegression>(_json_obj, _impl,
                                                _dependencies);
  } else if (type == "XGBoostPredictor" || type == "XGBoostClassifier" ||
             type == "XGBoostRegressor") {
    return std::make_shared<XGBoostPredictor>(_json_obj, _impl, _dependencies);
  } else {
    throw std::invalid_argument("Predictor of type '" + type + "' not known!");
  }
}

}  // namespace predictors
