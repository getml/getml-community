#include "relmt/Hyperparameters.hpp"

namespace relmt
{
// ----------------------------------------------------------------------------

Hyperparameters::Hyperparameters()
    : allow_avg_( true ),
      delta_t_( 0.3 ),
      gamma_( 1.0 ),
      loss_function_( "SquareLoss" ),
      max_depth_( 1 ),
      min_df_( 30 ),
      min_num_samples_( 200 ),
      num_features_( 10 ),
      num_subfeatures_( 10 ),
      num_threads_( 0 ),
      propositionalization_(
          std::shared_ptr<const fastprop::Hyperparameters>() ),
      reg_lambda_( 0.0 ),
      sampling_factor_( 1.0 ),
      seed_( 5843 ),
      shrinkage_( 0.3 ),
      silent_( true ),
      use_timestamps_( true ),
      vocab_size_( 500 )
{
}

// ----------------------------------------------------------------------------

Hyperparameters::Hyperparameters( const Poco::JSON::Object& _obj )
    : allow_avg_( JSON::get_value<bool>( _obj, "allow_avg_" ) ),
      delta_t_( JSON::get_value<Float>( _obj, "delta_t_" ) ),
      gamma_( JSON::get_value<Float>( _obj, "gamma_" ) ),
      loss_function_( JSON::get_value<std::string>( _obj, "loss_function_" ) ),
      max_depth_( JSON::get_value<Int>( _obj, "max_depth_" ) ),
      min_df_(
          _obj.has( "min_df_" )
              ? JSON::get_value<size_t>( _obj, "min_df_" )
              : static_cast<size_t>(
                    30 ) ),  // TODO: Check inserted for backwards
                             // compatability. Remove later.
      min_num_samples_( JSON::get_value<Int>( _obj, "min_num_samples_" ) ),
      num_features_( JSON::get_value<Int>( _obj, "num_features_" ) ),
      num_subfeatures_( JSON::get_value<Int>( _obj, "num_subfeatures_" ) ),
      num_threads_( JSON::get_value<Int>( _obj, "num_threads_" ) ),
      propositionalization_(
          _obj.has( "propositionalization_" )
              ? std::make_shared<const fastprop::Hyperparameters>(
                    *JSON::get_object( _obj, "propositionalization_" ) )
              : std::shared_ptr<const fastprop::Hyperparameters>() ),
      reg_lambda_( JSON::get_value<Float>( _obj, "reg_lambda_" ) ),
      sampling_factor_( JSON::get_value<Float>( _obj, "sampling_factor_" ) ),
      seed_( JSON::get_value<unsigned int>( _obj, "seed_" ) ),
      shrinkage_( JSON::get_value<Float>( _obj, "shrinkage_" ) ),
      silent_( JSON::get_value<bool>( _obj, "silent_" ) ),
      use_timestamps_( JSON::get_value<bool>( _obj, "use_timestamps_" ) ),
      vocab_size_(
          _obj.has( "vocab_size_" )
              ? JSON::get_value<size_t>( _obj, "vocab_size_" )
              : static_cast<size_t>(
                    500 ) )  // TODO: Check inserted for backwards
                             // compatability. Remove later.
{
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr Hyperparameters::to_json_obj() const
{
    // ---------------------------------------------------------

    Poco::JSON::Object::Ptr obj( new Poco::JSON::Object() );

    // ---------------------------------------------------------

    obj->set( "allow_avg_", allow_avg_ );

    obj->set( "delta_t_", delta_t_ );

    obj->set( "gamma_", gamma_ );

    obj->set( "loss_function_", loss_function_ );

    obj->set( "max_depth_", max_depth_ );

    obj->set( "min_df_", min_df_ );

    obj->set( "min_num_samples_", min_num_samples_ );

    obj->set( "num_features_", num_features_ );

    obj->set( "num_subfeatures_", num_subfeatures_ );

    obj->set( "num_threads_", num_threads_ );

    if ( propositionalization_ )
        {
            obj->set(
                "propositionalization_", propositionalization_->to_json_obj() );
        }

    obj->set( "reg_lambda_", reg_lambda_ );

    obj->set( "sampling_factor_", sampling_factor_ );

    obj->set( "seed_", seed_ );

    obj->set( "shrinkage_", shrinkage_ );

    obj->set( "silent_", silent_ );

    obj->set( "use_timestamps_", use_timestamps_ );

    obj->set( "vocab_size_", vocab_size_ );

    // ---------------------------------------------------------

    return obj;

    // ---------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace relmt
