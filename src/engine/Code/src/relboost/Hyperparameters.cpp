#include "relboost/Hyperparameters.hpp"

namespace relboost
{
// ----------------------------------------------------------------------------

Hyperparameters::Hyperparameters()
    : eta_( 0.3 ),
      gamma_( 1.0 ),
      lambda_( 0.0 ),
      max_depth_( 1 ),
      num_features_( 10 ),
      num_threads_( 0 ),
      objective_( "SquareLoss" ),
      seed_( 5843 ),
      silent_( true ),
      subsample_( 1.0 ),
      use_timestamps_( true )
{
}

// ----------------------------------------------------------------------------

Hyperparameters::Hyperparameters( const Poco::JSON::Object& _obj )
    : eta_( JSON::get_value<RELBOOST_FLOAT>( _obj, "eta_" ) ),
      gamma_( JSON::get_value<RELBOOST_FLOAT>( _obj, "gamma_" ) ),
      lambda_( JSON::get_value<RELBOOST_FLOAT>( _obj, "lambda_" ) ),
      max_depth_( JSON::get_value<RELBOOST_INT>( _obj, "max_depth_" ) ),
      num_features_( JSON::get_value<RELBOOST_INT>( _obj, "num_features_" ) ),
      num_threads_( JSON::get_value<RELBOOST_INT>( _obj, "num_threads_" ) ),
      objective_( JSON::get_value<std::string>( _obj, "objective_" ) ),
      seed_( JSON::get_value<size_t>( _obj, "seed_" ) ),
      silent_( JSON::get_value<bool>( _obj, "silent_" ) ),
      subsample_( JSON::get_value<RELBOOST_FLOAT>( _obj, "subsample_" ) ),
      use_timestamps_( JSON::get_value<bool>( _obj, "use_timestamps_" ) )
{
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Hyperparameters::to_json_obj() const
{
    Poco::JSON::Object obj;

    // ---------------------------------------------------------

    obj.set( "eta_", eta_ );

    obj.set( "gamma_", gamma_ );

    obj.set( "lambda_", lambda_ );

    obj.set( "max_depth_", max_depth_ );

    obj.set( "num_features_", num_features_ );

    obj.set( "num_threads_", num_threads_ );

    obj.set( "objective_", objective_ );

    obj.set( "seed_", seed_ );

    obj.set( "silent_", silent_ );

    obj.set( "subsample_", subsample_ );

    obj.set( "use_timestamps_", use_timestamps_ );

    // ---------------------------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------
}  // namespace relboost
