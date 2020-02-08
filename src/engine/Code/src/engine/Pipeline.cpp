#include "engine/pipelines/pipelines.hpp"

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------

void Pipeline::add_population_cols(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    containers::Features* _features ) const
{
    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    for ( const auto& col : predictor_impl().numerical_colnames() )
        {
            _features->push_back( population_df.numerical( col ).data_ptr() );
        }
}

// ----------------------------------------------------------------------

void Pipeline::calculate_feature_stats(
    const containers::Features _features,
    const size_t _nrows,
    const size_t _ncols,
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames )
{
    // ------------------------------------------------------------------------

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto df = utils::Getter::get( population_name, _data_frames );

    // ------------------------------------------------------------------------

    std::vector<const Float*> targets;

    for ( size_t j = 0; j < df.num_targets(); ++j )
        {
            targets.push_back( df.target( j ).data() );
        }

    // ------------------------------------------------------------------------

    const size_t num_bins = 200;

    scores_.from_json_obj( metrics::Summarizer::calculate_feature_correlations(
        _features, _nrows, _ncols, targets ) );

    scores_.from_json_obj( metrics::Summarizer::calculate_feature_plots(
        _features, _nrows, _ncols, num_bins, targets ) );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

containers::Features Pipeline::generate_predictions(
    const containers::CategoricalFeatures& _categorical_features,
    const containers::Features& _numerical_features ) const
{
    // ------------------------------------------------------------------------

    size_t nrows = 0;

    if ( _numerical_features.size() > 0 )
        {
            nrows = _numerical_features[0]->size();
        }
    else if ( _categorical_features.size() > 0 )
        {
            nrows = _categorical_features[0]->size();
        }
    else
        {
            assert_true( false && "No features" );
        }

    // ------------------------------------------------------------------------

    assert_true( num_predictors_per_set() > 0 );

    const auto divisor = static_cast<Float>( num_predictors_per_set() );

    auto predictions = containers::Features();

    for ( size_t i = 0; i < num_predictor_sets(); ++i )
        {
            auto mean_prediction =
                std::make_shared<std::vector<Float>>( nrows );

            for ( size_t j = 0; j < num_predictors_per_set(); ++j )
                {
                    const auto new_prediction = predictor( i, j )->predict(
                        _categorical_features, _numerical_features );

                    assert_true( new_prediction );
                    assert_true(
                        new_prediction->size() == mean_prediction->size() );

                    std::transform(
                        mean_prediction->begin(),
                        mean_prediction->end(),
                        new_prediction->begin(),
                        mean_prediction->begin(),
                        std::plus<Float>() );
                }

            for ( auto& val : *mean_prediction )
                {
                    val /= divisor;
                }

            predictions.push_back( mean_prediction );
        }

    // ------------------------------------------------------------------------

    return predictions;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

containers::CategoricalFeatures Pipeline::get_categorical_features(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames ) const
{
    auto categorical_features = containers::CategoricalFeatures();

    // TODO
    /*if ( !feature_engineerer().hyperparameters().include_categorical_ )
        {
            return categorical_features;
        }*/

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    for ( const auto& col : predictor_impl().categorical_colnames() )
        {
            categorical_features.push_back(
                population_df.categorical( col ).data_ptr() );
        }

    return categorical_features;
}

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

// ----------------------------------------------------------------------------

containers::Features Pipeline::transform(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------------------------
    // Generate the features.

    auto numerical_features = containers::Features();

    for ( auto& fe : feature_engineerers_ )
        {
            assert_true( fe );

            auto new_features =
                fe->transform( _cmd, _logger, _data_frames, _socket );

            numerical_features.insert(
                numerical_features.end(),
                new_features.begin(),
                new_features.end() );
        }

    // -------------------------------------------------------------------------
    // Add the numerical columns from the population table.

    add_population_cols( _cmd, _data_frames, &numerical_features );

    // -------------------------------------------------------------------------
    // If we do not want to score or predict, then we can stop here.

    const bool score =
        _cmd.has( "score_" ) && JSON::get_value<bool>( _cmd, "score_" );

    const bool predict =
        _cmd.has( "predict_" ) && JSON::get_value<bool>( _cmd, "predict_" );

    if ( !score && !predict )
        {
            return numerical_features;
        }

    // -------------------------------------------------------------------------
    // Retrieve the categorical features.

    auto categorical_features = get_categorical_features( _cmd, _data_frames );

    categorical_features =
        predictor_impl().transform_encodings( categorical_features );

    // -------------------------------------------------------------------------
    // Calculate the feature statistics, if applicable.

    const auto ncols = numerical_features.size();

    if ( score && ncols > 0 )
        {
            const auto nrows = numerical_features[0]->size();

            calculate_feature_stats(
                numerical_features, nrows, ncols, _cmd, _data_frames );
        }

    //-------------------------------------------------------------------------
    // Generate predictions, if applicable.

    if ( predict && num_predictors_per_set() > 0 )
        {
            return generate_predictions(
                categorical_features, numerical_features );
        }
    else
        {
            return numerical_features;
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
