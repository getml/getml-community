#include "relboost/Hyperparameters.hpp"

namespace relboost
{
// ----------------------------------------------------------------------------

Hyperparameters::Hyperparameters()
    : allow_null_weights_( false ),
      delta_t_( 0.3 ),
      gamma_( 1.0 ),
      include_categorical_( true ),
      loss_function_( "SquareLoss" ),
      max_depth_( 1 ),
      min_num_samples_( 200 ),
      num_features_( 10 ),
      num_subfeatures_( 10 ),
      num_threads_( 0 ),
      reg_lambda_( 0.0 ),
      sampling_factor_( 1.0 ),
      seed_( 5843 ),
      session_name_( "" ),
      share_selected_features_( 0.0 ),
      shrinkage_( 0.3 ),
      silent_( true ),
      target_num_( 0 ),
      use_timestamps_( true )
{
}

// ----------------------------------------------------------------------------

Hyperparameters::Hyperparameters( const Poco::JSON::Object& _obj )
    : allow_null_weights_(
          _obj.has( "allow_null_weights_" )
              ? JSON::get_value<bool>( _obj, "allow_null_weights_" )
              : true ),  // TODO: Check inserted for backwards compatability.
                         // Remove later.
      delta_t_( JSON::get_value<Float>( _obj, "delta_t_" ) ),
      /*feature_selector_(
          _obj.has( "feature_selector_" )
              ? _obj.getObject( "feature_selector_" )
              : nullptr ),*/  // TODO: Remove
      gamma_( JSON::get_value<Float>( _obj, "gamma_" ) ),
      include_categorical_(
          _obj.has( "include_categorical_" )
              ? JSON::get_value<bool>( _obj, "include_categorical_" )
              : true ),  // TODO: Remove
      loss_function_( JSON::get_value<std::string>( _obj, "loss_function_" ) ),
      max_depth_( JSON::get_value<Int>( _obj, "max_depth_" ) ),
      min_num_samples_( JSON::get_value<Int>( _obj, "min_num_samples_" ) ),
      num_features_( JSON::get_value<Int>( _obj, "num_features_" ) ),
      num_subfeatures_( JSON::get_value<Int>( _obj, "num_subfeatures_" ) ),
      num_threads_( JSON::get_value<Int>( _obj, "num_threads_" ) ),
      /*predictor_(
          _obj.has( "predictor_" ) ? _obj.getObject( "predictor_" ) : nullptr ),*/ //TODO: Remove 
      reg_lambda_( JSON::get_value<Float>( _obj, "reg_lambda_" ) ),
      sampling_factor_( JSON::get_value<Float>( _obj, "sampling_factor_" ) ),
      seed_( JSON::get_value<unsigned int>( _obj, "seed_" ) ),
      session_name_(
          _obj.has( "session_name_" )
              ? JSON::get_value<std::string>( _obj, "session_name_" )
              : "" ),  // TODO: Check inserted for backwards compatability.
                       // Remove later.
      share_selected_features_(
          _obj.has( "share_selected_features_" )
              ? JSON::get_value<Float>( _obj, "share_selected_features_" )
              : 1.0 ),  // TODO: Remove
      shrinkage_( JSON::get_value<Float>( _obj, "shrinkage_" ) ),
      silent_( JSON::get_value<bool>( _obj, "silent_" ) ),
      target_num_( /*JSON::get_value<Int>( _obj, "target_num_" )*/ 0 ), // TODO
      use_timestamps_( JSON::get_value<bool>( _obj, "use_timestamps_" ) )
{
}

// ----------------------------------------------------------------------------

size_t Hyperparameters::num_selected_features() const
{
    if ( share_selected_features_ <= 0.0 || share_selected_features_ >= 1.0 )
        {
            return num_features_;
        }
    else
        {
            const auto num_selected =
                static_cast<size_t>( share_selected_features_ * num_features_ );

            if ( num_selected == 0 )
                {
                    return 1;
                }
            else
                {
                    return num_selected;
                }
        }
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr Hyperparameters::to_json_obj() const
{
    // ---------------------------------------------------------

    Poco::JSON::Object::Ptr obj( new Poco::JSON::Object() );

    // ---------------------------------------------------------

    obj->set( "allow_null_weights_", allow_null_weights_ );

    obj->set( "delta_t_", delta_t_ );

    if ( feature_selector_ )
        {
            obj->set( "feature_selector_", feature_selector_ );
        }

    obj->set( "gamma_", gamma_ );

    obj->set( "include_categorical_", include_categorical_ );

    obj->set( "loss_function_", loss_function_ );

    obj->set( "max_depth_", max_depth_ );

    obj->set( "min_num_samples_", min_num_samples_ );

    obj->set( "num_features_", num_features_ );

    obj->set( "num_subfeatures_", num_subfeatures_ );

    obj->set( "num_threads_", num_threads_ );

    if ( predictor_ )
        {
            obj->set( "predictor_", predictor_ );
        }

    obj->set( "reg_lambda_", reg_lambda_ );

    obj->set( "sampling_factor_", sampling_factor_ );

    obj->set( "seed_", seed_ );

    obj->set( "session_name_", session_name_ );

    obj->set( "share_selected_features_", share_selected_features_ );

    obj->set( "shrinkage_", shrinkage_ );

    obj->set( "silent_", silent_ );

    obj->set( "use_timestamps_", use_timestamps_ );

    // ---------------------------------------------------------

    return obj;

    // ---------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace relboost
