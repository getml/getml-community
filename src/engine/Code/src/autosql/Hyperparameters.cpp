#include "autosql/descriptors/descriptors.hpp"

namespace autosql
{
namespace descriptors
{
// ----------------------------------------------------------------------------

Hyperparameters::Hyperparameters( const Poco::JSON::Object& _json_obj )
    : aggregations_( JSON::array_to_vector<std::string>(
          JSON::get_array( _json_obj, "aggregation_" ) ) ),
      fast_training_( JSON::get_value<bool>( _json_obj, "fast_training_" ) ),
      include_categorical_(
          JSON::get_value<bool>( _json_obj, "include_categorical_" ) ),
      loss_function_(
          JSON::get_value<std::string>( _json_obj, "loss_function_" ) ),
      num_features_( JSON::get_value<size_t>( _json_obj, "num_features_" ) ),
      num_selected_features_(
          Hyperparameters::calc_num_selected_features( _json_obj ) ),
      num_subfeatures_(
          JSON::get_value<size_t>( _json_obj, "num_subfeatures_" ) ),
      num_threads_( JSON::get_value<size_t>( _json_obj, "num_threads_" ) ),
      round_robin_( JSON::get_value<bool>( _json_obj, "round_robin_" ) ),
      sampling_factor_(
          JSON::get_value<size_t>( _json_obj, "sampling_factor_" ) ),
      seed_( JSON::get_value<size_t>( _json_obj, "seed_" ) ),
      share_aggregations_(
          JSON::get_value<Float>( _json_obj, "share_aggregations_" ) ),
      shrinkage_( JSON::get_value<Float>( _json_obj, "shrinkage_" ) ),
      tree_hyperparameters_(
          std::make_shared<const TreeHyperparameters>( _json_obj ) ),
      use_timestamps_( JSON::get_value<bool>( _json_obj, "use_timestamps_" ) )
{
}

// ----------------------------------------------------------------------------

size_t Hyperparameters::calc_num_selected_features(
    const Poco::JSON::Object& _json_obj )
{
    const auto nsf =
        JSON::get_value<size_t>( _json_obj, "num_selected_features_" );

    if ( _json_obj.has( "feature_selector_" ) && nsf <= 0 )
        {
            return JSON::get_value<size_t>( _json_obj, "num_features_" );
        }
    else
        {
            return nsf;
        }
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Hyperparameters::to_json_obj() const
{
    // -------------------------

    Poco::JSON::Object obj;

    // -------------------------

    obj.set( "aggregation_", aggregations_ );

    obj.set( "allow_sets_", tree_hyperparameters_->allow_sets_ );

    obj.set( "include_categorical_", include_categorical_ );

    obj.set( "loss_function_", loss_function_ );

    obj.set( "use_timestamps_", use_timestamps_ );

    obj.set( "num_features_", num_features_ );

    obj.set( "num_selected_features_", num_selected_features_ );

    obj.set( "num_subfeatures_", num_subfeatures_ );

    obj.set( "max_length_", tree_hyperparameters_->max_length_ );

    obj.set( "fast_training_", fast_training_ );

    obj.set( "min_num_samples_", tree_hyperparameters_->min_num_samples_ );

    obj.set( "shrinkage_", shrinkage_ );

    obj.set( "sampling_factor_", sampling_factor_ );

    obj.set( "round_robin_", round_robin_ );

    obj.set( "share_aggregations_", share_aggregations_ );

    obj.set( "share_conditions_", tree_hyperparameters_->share_conditions_ );

    obj.set( "grid_factor_", tree_hyperparameters_->grid_factor_ );

    obj.set( "regularization_", tree_hyperparameters_->regularization_ );

    obj.set( "seed_", seed_ );

    obj.set( "num_threads_", num_threads_ );

    // -------------------------

    return obj;

    // -------------------------
}

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace autosql
