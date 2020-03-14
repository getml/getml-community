#include "engine/featureengineerers/featureengineerers.hpp"

namespace engine
{
namespace featureengineerers
{
// ----------------------------------------------------------------------

std::shared_ptr<AbstractFeatureEngineerer> FeatureEngineererParser::parse(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const std::vector<strings::String>>& _categories )
{
    const auto type = JSON::get_value<std::string>( _cmd, "type_" );

    if ( type == "MultirelModel" )
        {
            return std::make_shared<
                FeatureEngineerer<multirel::ensemble::DecisionTreeEnsemble>>(
                _categories, _cmd );
        }
    else if ( type == "RelboostModel" )
        {
            return std::make_shared<
                FeatureEngineerer<multirel::ensemble::DecisionTreeEnsemble>>(
                _categories, _cmd );
        }
    else
        {
            throw std::invalid_argument(
                "Feature engineering algorithm of type '" + type +
                "' not known!" );

            return std::shared_ptr<AbstractFeatureEngineerer>();
        }
}

// ----------------------------------------------------------------------
}  // namespace featureengineerers
}  // namespace engine
