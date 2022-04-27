#include "predictors/PredictorParser.hpp"

// ----------------------------------------------------------------------

#include "predictors/LinearRegression.hpp"
#include "predictors/LogisticRegression.hpp"
#include "predictors/XGBoostPredictor.hpp"

// ----------------------------------------------------------------------

namespace predictors {

fct::Ref<Predictor> PredictorParser::parse(
    const Poco::JSON::Object& _json_obj,
    const std::shared_ptr<const PredictorImpl>& _impl,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies) {
  const auto type = JSON::get_value<std::string>(_json_obj, "type_");

  if (type == "LinearRegression") {
    return fct::Ref<LinearRegression>::make(_json_obj, _impl, _dependencies);
  } else if (type == "LogisticRegression") {
    return fct::Ref<LogisticRegression>::make(_json_obj, _impl, _dependencies);
  } else if (type == "XGBoostPredictor" || type == "XGBoostClassifier" ||
             type == "XGBoostRegressor") {
    return fct::Ref<XGBoostPredictor>::make(_json_obj, _impl, _dependencies);
  } else {
    throw std::runtime_error("Predictor of type '" + type + "' not known!");
  }
}

}  // namespace predictors
