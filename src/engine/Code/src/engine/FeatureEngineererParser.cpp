#include "engine/featureengineerers/featureengineerers.hpp"

namespace engine
{
namespace featureengineerers
{
// ----------------------------------------------------------------------

containers::Optional<AbstractFeatureEngineerer> FeatureEngineererParser::parse(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const std::vector<strings::String>>& _categories )
{
    const auto type = JSON::get_value<std::string>( _cmd, "type_" );

    if ( type == "MultirelModel" )
        {
            return containers::Optional<AbstractFeatureEngineerer>(
                new FeatureEngineerer<multirel::ensemble::DecisionTreeEnsemble>(
                    _categories, _cmd ) );
        }
    else if ( type == "RelboostModel" )
        {
            return containers::Optional<AbstractFeatureEngineerer>(
                new FeatureEngineerer<relboost::ensemble::DecisionTreeEnsemble>(
                    _categories, _cmd ) );
        }
    else
        {
            throw std::invalid_argument(
                "Feature engineering algorithm of type '" + type +
                "' not known!" );

            return containers::Optional<AbstractFeatureEngineerer>();
        }
}

// ----------------------------------------------------------------------
}  // namespace featureengineerers
}  // namespace engine
