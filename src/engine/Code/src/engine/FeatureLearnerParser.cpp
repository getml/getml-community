#include "engine/featurelearners/featurelearners.hpp"

namespace engine
{
namespace featurelearners
{
// ----------------------------------------------------------------------

std::shared_ptr<AbstractFeatureLearner> FeatureLearnerParser::parse(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const helpers::Placeholder>& _placeholder,
    const std::shared_ptr<const std::vector<std::string>>& _peripheral,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies )
{
    const auto type = JSON::get_value<std::string>( _cmd, "type_" );

    if ( type == AbstractFeatureLearner::FASTPROP_MODEL )
        {
            return std::make_shared<
                FeatureLearner<fastprop::algorithm::FastProp>>(
                _cmd, _placeholder, _peripheral, _dependencies );
        }

    if ( type == AbstractFeatureLearner::FASTPROP_TIME_SERIES )
        {
            return std::make_shared<FeatureLearner<ts::FastPropTimeSeries>>(
                _cmd, _placeholder, _peripheral, _dependencies );
        }

    if ( type == AbstractFeatureLearner::MULTIREL_MODEL )
        {
            return std::make_shared<
                FeatureLearner<multirel::ensemble::DecisionTreeEnsemble>>(
                _cmd, _placeholder, _peripheral, _dependencies );
        }

    if ( type == AbstractFeatureLearner::MULTIREL_TIME_SERIES )
        {
            return std::make_shared<FeatureLearner<ts::MultirelTimeSeries>>(
                _cmd, _placeholder, _peripheral, _dependencies );
        }

    if ( type == AbstractFeatureLearner::RELBOOST_MODEL )
        {
            return std::make_shared<
                FeatureLearner<relboost::ensemble::DecisionTreeEnsemble>>(
                _cmd, _placeholder, _peripheral, _dependencies );
        }

    if ( type == AbstractFeatureLearner::RELMT_MODEL )
        {
            return std::make_shared<
                FeatureLearner<relmt::ensemble::DecisionTreeEnsemble>>(
                _cmd, _placeholder, _peripheral, _dependencies );
        }

    if ( type == AbstractFeatureLearner::RELBOOST_TIME_SERIES )
        {
            return std::make_shared<FeatureLearner<ts::RelboostTimeSeries>>(
                _cmd, _placeholder, _peripheral, _dependencies );
        }

    if ( type == AbstractFeatureLearner::RELMT_TIME_SERIES )
        {
            return std::make_shared<FeatureLearner<ts::RelMTTimeSeries>>(
                _cmd, _placeholder, _peripheral, _dependencies );
        }

    throw std::invalid_argument(
        "Feature learning algorithm of type '" + type + "' not known!" );

    return std::shared_ptr<AbstractFeatureLearner>();
}

// ----------------------------------------------------------------------
}  // namespace featurelearners
}  // namespace engine
