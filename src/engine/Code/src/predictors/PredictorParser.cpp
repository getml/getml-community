#include "predictors/predictors.hpp"

namespace predictors
{
// ----------------------------------------------------------------------

std::shared_ptr<Predictor> PredictorParser::parse(
    const Poco::JSON::Object& _json_obj )
{
    const auto type = JSON::get_value<std::string>( _json_obj, "type_" );

    if ( type == "XGBoostPredictor" )
        {
            return std::make_shared<XGBoostPredictor>(
                XGBoostHyperparams( _json_obj ) );
        }
    else
        {
            throw std::invalid_argument(
                "Predictor of type '" + type + "' not known!" );
        }
}

// ----------------------------------------------------------------------
}  // namespace predictors
