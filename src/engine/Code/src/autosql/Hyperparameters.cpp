#include "descriptors/descriptors.hpp"

namespace autosql
{
namespace descriptors
{
// ----------------------------------------------------------------------------

Hyperparameters::Hyperparameters( const Poco::JSON::Object& _json_obj )
    : aggregations( JSON::array_to_vector<std::string>(
          _json_obj.AUTOSQL_GET_ARRAY( "aggregation_" ) ) ),
      fast_training( _json_obj.AUTOSQL_GET_VALUE<bool>( "fast_training_" ) ),
      feature_selector_hyperparams(
          Hyperparameters::parse_feature_selector( _json_obj ) ),
      loss_function(
          _json_obj.AUTOSQL_GET_VALUE<std::string>( "loss_function_" ) ),
      num_features( _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_INT>( "num_features_" ) ),
      num_selected_features(
          Hyperparameters::calc_num_selected_features( _json_obj ) ),
      num_subfeatures(
          _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_INT>( "num_subfeatures_" ) ),
      num_threads( _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_INT>( "num_threads_" ) ),
      predictor_hyperparams( Hyperparameters::parse_predictor( _json_obj ) ),
      round_robin( _json_obj.AUTOSQL_GET_VALUE<bool>( "round_robin_" ) ),
      sampling_factor(
          _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_INT>( "sampling_factor_" ) ),
      seed( _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_INT>( "seed_" ) ),
      share_aggregations(
          _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_FLOAT>( "share_aggregations_" ) ),
      shrinkage( _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_FLOAT>( "shrinkage_" ) ),
      tree_hyperparameters( TreeHyperparameters( _json_obj ) ),
      use_timestamps( _json_obj.AUTOSQL_GET_VALUE<bool>( "use_timestamps_" ) )
{
    if ( !feature_selector_hyperparams && num_selected_features > 0 )
        {
            throw std::invalid_argument(
                "If you want feature selection, you need to pass a feature "
                "selector. Please pass a feature selector or set the "
                "number of selected features to zero." );
        }
}

// ----------------------------------------------------------------------------

AUTOSQL_INT Hyperparameters::calc_num_selected_features(
    const Poco::JSON::Object& _json_obj )
{
    const auto nsf =
        _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_INT>( "num_selected_features_" );

    if ( _json_obj.has( "feature_selector_" ) && nsf <= 0 )
        {
            return _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_INT>( "num_features_" );
        }
    else
        {
            return nsf;
        }
}

// ----------------------------------------------------------------------------

containers::Optional<const Poco::JSON::Object>
Hyperparameters::parse_feature_selector( const Poco::JSON::Object& _json_obj )
{
    containers::Optional<const Poco::JSON::Object> optional;

    if ( _json_obj.has( "feature_selector_" ) )
        {
            optional.reset( new Poco::JSON::Object(
                *_json_obj.AUTOSQL_GET_OBJECT( "feature_selector_" ) ) );
        }

    return optional;
}

// ----------------------------------------------------------------------------

containers::Optional<const Poco::JSON::Object> Hyperparameters::parse_predictor(
    const Poco::JSON::Object& _json_obj )
{
    containers::Optional<const Poco::JSON::Object> optional;

    if ( _json_obj.has( "predictor_" ) )
        {
            optional.reset( new Poco::JSON::Object(
                *_json_obj.AUTOSQL_GET_OBJECT( "predictor_" ) ) );
        }

    return optional;
}

// ----------------------------------------------------------------------------

/// Transforms the hyperparameters into a JSON object
Poco::JSON::Object Hyperparameters::to_json_obj() const
{
    // -------------------------

    Poco::JSON::Object obj;

    // -------------------------

    obj.set( "aggregation_", aggregations );

    obj.set( "allow_sets_", tree_hyperparameters.allow_sets );

    obj.set( "loss_function_", loss_function );

    obj.set( "use_timestamps_", use_timestamps );

    obj.set( "num_features_", num_features );

    obj.set( "num_selected_features_", num_selected_features );

    obj.set( "num_subfeatures_", num_subfeatures );

    obj.set( "max_length_", tree_hyperparameters.max_length );

    obj.set( "fast_training_", fast_training );

    obj.set( "min_num_samples_", tree_hyperparameters.min_num_samples );

    obj.set( "shrinkage_", shrinkage );

    obj.set( "sampling_factor_", sampling_factor );

    obj.set( "round_robin_", round_robin );

    obj.set( "share_aggregations_", share_aggregations );

    obj.set( "share_conditions_", tree_hyperparameters.share_conditions );

    obj.set( "grid_factor_", tree_hyperparameters.grid_factor );

    obj.set( "regularization_", tree_hyperparameters.regularization );

    obj.set( "seed_", seed );

    obj.set( "num_threads_", num_threads );

    // -------------------------

    if ( feature_selector_hyperparams )
        {
            obj.set( "feature_selector_", *feature_selector_hyperparams );
        }

    // -------------------------

    if ( predictor_hyperparams )
        {
            obj.set( "predictor_", *predictor_hyperparams );
        }

    // -------------------------

    return obj;
}

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace autosql
