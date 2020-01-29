#include "engine/pipelines/pipelines.hpp"

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------

std::vector<containers::Optional<featureengineerers::AbstractFeatureEngineerer>>
Pipeline::make_feature_engineerers() const
{
    const auto arr = JSON::get_array( obj_, "feature_engineerers_" );

    std::vector<
        containers::Optional<featureengineerers::AbstractFeatureEngineerer>>
        feature_engineerers;

    for ( size_t i = 0; i < arr->size(); ++i )
        {
            const auto ptr = arr->getObject( i );

            if ( !ptr )
                {
                    throw std::invalid_argument(
                        "Element " + std::to_string( i ) +
                        " in feature_engineerers_ is not a proper JSON "
                        "object." );
                }

            feature_engineerers.push_back(
                featureengineerers::FeatureEngineererParser::parse(
                    *ptr, categories_ ) );
        }

    return feature_engineerers;
}

// ----------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
