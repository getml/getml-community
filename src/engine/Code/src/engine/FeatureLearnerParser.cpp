#include "engine/featurelearners/featurelearners.hpp"

namespace engine
{
namespace featurelearners
{
// ----------------------------------------------------------------------

std::shared_ptr<AbstractFeatureLearner> FeatureLearnerParser::parse(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const Poco::JSON::Object>& _placeholder,
    const std::shared_ptr<const std::vector<std::string>>& _peripheral,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies )
{
    const auto type = JSON::get_value<std::string>( _cmd, "type_" );

    if ( type == "MultirelModel" )
        {
            return std::make_shared<
                FeatureLearner<multirel::ensemble::DecisionTreeEnsemble>>(
                _cmd, _placeholder, _peripheral, _dependencies );
        }
    else if ( type == "MultirelTimeSeries" )
        {
            return std::make_shared<FeatureLearner<ts::MultirelTimeSeries>>(
                _cmd, _placeholder, _peripheral, _dependencies );
        }
    else if ( type == "RelboostModel" )
        {
            return std::make_shared<
                FeatureLearner<relboost::ensemble::DecisionTreeEnsemble>>(
                _cmd, _placeholder, _peripheral, _dependencies );
        }
    else if ( type == "RelboostTimeSeries" )
        {
            return std::make_shared<FeatureLearner<ts::RelboostTimeSeries>>(
                _cmd, _placeholder, _peripheral, _dependencies );
        }
    else
        {
            throw std::invalid_argument(
                "Feature learning algorithm of type '" + type +
                "' not known!" );

            return std::shared_ptr<AbstractFeatureLearner>();
        }
}

// ----------------------------------------------------------------------
}  // namespace featurelearners
}  // namespace engine
