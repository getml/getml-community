#include "relboost/Hyperparameters.hpp"

namespace relboost
{
// ----------------------------------------------------------------------------

Hyperparameters::Hyperparameters()
    : eta_( 0.3 ),
      gamma_( 1.0 ),
      include_categorical_( true ),
      lambda_( 0.0 ),
      max_depth_( 1 ),
      min_num_samples_( 200 ),
      num_features_( 10 ),
      num_selected_features_( 10 ),
      num_threads_( 0 ),
      objective_( "SquareLoss" ),
      sampling_factor_( 1.0 ),
      seed_( 5843 ),
      silent_( true ),
      use_timestamps_( true )
{
}

// ----------------------------------------------------------------------------

Hyperparameters::Hyperparameters( const Poco::JSON::Object& _obj )
    : eta_( JSON::get_value<Float>( _obj, "eta_" ) ),
      gamma_( JSON::get_value<Float>( _obj, "gamma_" ) ),
      include_categorical_(
          JSON::get_value<bool>( _obj, "include_categorical_" ) ),
      lambda_( JSON::get_value<Float>( _obj, "lambda_" ) ),
      max_depth_( JSON::get_value<Int>( _obj, "max_depth_" ) ),
      min_num_samples_( JSON::get_value<Int>( _obj, "min_num_samples_" ) ),
      num_features_( JSON::get_value<Int>( _obj, "num_features_" ) ),
      num_selected_features_(
          JSON::get_value<Int>( _obj, "num_selected_features_" ) ),
      num_threads_( JSON::get_value<Int>( _obj, "num_threads_" ) ),
      objective_( JSON::get_value<std::string>( _obj, "objective_" ) ),
      sampling_factor_( JSON::get_value<Float>( _obj, "sampling_factor_" ) ),
      seed_( JSON::get_value<size_t>( _obj, "seed_" ) ),
      silent_( JSON::get_value<bool>( _obj, "silent_" ) ),
      use_timestamps_( JSON::get_value<bool>( _obj, "use_timestamps_" ) )
{
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Hyperparameters::to_json_obj() const
{
    // ---------------------------------------------------------

    Poco::JSON::Object obj;

    // ---------------------------------------------------------

    obj.set( "eta_", eta_ );

    obj.set( "gamma_", gamma_ );

    obj.set( "include_categorical_", include_categorical_ );

    obj.set( "lambda_", lambda_ );

    obj.set( "max_depth_", max_depth_ );

    obj.set( "min_num_samples_", min_num_samples_ );

    obj.set( "num_features_", num_features_ );

    obj.set( "num_selected_features_", num_selected_features_ );

    obj.set( "num_threads_", num_threads_ );

    obj.set( "objective_", objective_ );

    obj.set( "sampling_factor_", sampling_factor_ );

    obj.set( "seed_", seed_ );

    obj.set( "silent_", silent_ );

    obj.set( "use_timestamps_", use_timestamps_ );

    // ---------------------------------------------------------

    return obj;

    // ---------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace relboost
