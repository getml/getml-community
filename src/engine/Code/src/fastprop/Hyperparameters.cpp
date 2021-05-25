#include "fastprop/Hyperparameters.hpp"

namespace fastprop
{
// ----------------------------------------------------------------------------

Hyperparameters::Hyperparameters( const Poco::JSON::Object& _json_obj )
    : aggregations_( jsonutils::JSON::array_to_vector<std::string>(
          jsonutils::JSON::get_array( _json_obj, "aggregation_" ) ) ),
      delta_t_(
          _json_obj.has( "delta_t_" )
              ? jsonutils::JSON::get_value<Float>( _json_obj, "delta_t_" )
              : 0.0 ),
      loss_function_( jsonutils::JSON::get_value<std::string>(
          _json_obj, "loss_function_" ) ),
      max_lag_(
          _json_obj.has( "max_lag_" )
              ? jsonutils::JSON::get_value<size_t>( _json_obj, "max_lag_" )
              : static_cast<size_t>( 0 ) ),
      min_df_( jsonutils::JSON::get_value<size_t>( _json_obj, "min_df_" ) ),
      n_most_frequent_(
          jsonutils::JSON::get_value<size_t>( _json_obj, "n_most_frequent_" ) ),
      num_features_(
          jsonutils::JSON::get_value<Int>( _json_obj, "num_features_" ) ),
      num_threads_(
          jsonutils::JSON::get_value<Int>( _json_obj, "num_threads_" ) ),
      sampling_factor_(
          _json_obj.has( "sampling_factor_" )
              ? jsonutils::JSON::get_value<Float>(
                    _json_obj, "sampling_factor_" )
              : 1.0 ),
      silent_( jsonutils::JSON::get_value<bool>( _json_obj, "silent_" ) ),
      vocab_size_(
          jsonutils::JSON::get_value<size_t>( _json_obj, "vocab_size_" ) )
{
    assert_true(
        loss_function_ == CROSS_ENTROPY_LOSS || loss_function_ == SQUARE_LOSS );
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr Hyperparameters::to_json_obj() const
{
    // -------------------------

    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    // -------------------------

    obj->set(
        "aggregation_", jsonutils::JSON::vector_to_array_ptr( aggregations_ ) );

    obj->set( "delta_t_", delta_t_ );

    obj->set( "loss_function_", loss_function_ );

    obj->set( "max_lag_", max_lag_ );

    obj->set( "min_df_", min_df_ );

    obj->set( "n_most_frequent_", n_most_frequent_ );

    obj->set( "num_features_", num_features_ );

    obj->set( "num_threads_", num_threads_ );

    obj->set( "sampling_factor_", sampling_factor_ );

    obj->set( "silent_", silent_ );

    obj->set( "vocab_size_", vocab_size_ );

    // -------------------------

    return obj;

    // -------------------------
}

// ----------------------------------------------------------------------------
}  // namespace fastprop
