#include "multirel/descriptors/descriptors.hpp"

namespace multirel
{
namespace descriptors
{
// ----------------------------------------------------------------------------

Hyperparameters::Hyperparameters( const Poco::JSON::Object& _json_obj )
    : aggregations_( JSON::array_to_vector<std::string>(
          JSON::get_array( _json_obj, "aggregation_" ) ) ),
      include_categorical_(
          _json_obj.has( "include_categorical_" )
              ? JSON::get_value<bool>( _json_obj, "include_categorical_" )
              : true ),  // TODO: Remove
      loss_function_(
          JSON::get_value<std::string>( _json_obj, "loss_function_" ) ),
      min_df_(
          _json_obj.has( "min_df_" )
              ? JSON::get_value<size_t>( _json_obj, "min_df_" )
              : static_cast<size_t>(
                    30 ) ),  // TODO: Check inserted for backwards
                             // compatability. Remove later.
      min_freq_(
          _json_obj.has( "min_freq_" )
              ? JSON::get_value<size_t>( _json_obj, "min_freq_" )
              : static_cast<size_t>(
                    0 ) ),  // TODO: Check inserted for backwards
                            // compatability. Remove later.
      num_features_( JSON::get_value<size_t>( _json_obj, "num_features_" ) ),
      num_subfeatures_(
          JSON::get_value<size_t>( _json_obj, "num_subfeatures_" ) ),
      num_threads_( JSON::get_value<size_t>( _json_obj, "num_threads_" ) ),
      propositionalization_(
          _json_obj.has( "propositionalization_" )
              ? std::make_shared<const fastprop::Hyperparameters>(
                    *JSON::get_object( _json_obj, "propositionalization_" ) )
              : std::shared_ptr<const fastprop::Hyperparameters>() ),
      round_robin_( JSON::get_value<bool>( _json_obj, "round_robin_" ) ),
      sampling_factor_(
          JSON::get_value<Float>( _json_obj, "sampling_factor_" ) ),
      seed_( JSON::get_value<unsigned int>( _json_obj, "seed_" ) ),
      share_aggregations_(
          JSON::get_value<Float>( _json_obj, "share_aggregations_" ) ),
      share_selected_features_(
          _json_obj.has( "share_selected_features_" )
              ? JSON::get_value<Float>( _json_obj, "share_selected_features_" )
              : 1.0 ),  // TODO: Remove
      shrinkage_( JSON::get_value<Float>( _json_obj, "shrinkage_" ) ),
      silent_(
          _json_obj.has( "silent_" )  // for backwards compatability
              ? JSON::get_value<bool>( _json_obj, "silent_" )
              : false ),
      split_text_fields_(
          _json_obj.has( "split_text_fields_" )
              ? JSON::get_value<bool>( _json_obj, "split_text_fields_" )
              : false ),  // TODO: Remove
      tree_hyperparameters_(
          std::make_shared<const TreeHyperparameters>( _json_obj ) ),
      use_timestamps_( JSON::get_value<bool>( _json_obj, "use_timestamps_" ) ),
      vocab_size_(
          _json_obj.has( "vocab_size_" )
              ? JSON::get_value<size_t>( _json_obj, "vocab_size_" )
              : static_cast<size_t>(
                    500 ) )  // TODO: Check inserted for backwards
                             // compatability. Remove later.
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
    // -------------------------

    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    // -------------------------

    obj->set( "aggregation_", aggregations_ );

    obj->set( "allow_sets_", tree_hyperparameters_->allow_sets_ );

    obj->set( "delta_t_", tree_hyperparameters_->delta_t_ );

    obj->set( "include_categorical_", include_categorical_ );

    obj->set( "loss_function_", loss_function_ );

    obj->set( "use_timestamps_", use_timestamps_ );

    obj->set( "num_features_", num_features_ );

    obj->set( "num_subfeatures_", num_subfeatures_ );

    obj->set( "max_length_", tree_hyperparameters_->max_length_ );

    obj->set( "min_df_", min_df_ );

    obj->set( "min_freq_", min_freq_ );

    obj->set( "min_num_samples_", tree_hyperparameters_->min_num_samples_ );

    obj->set( "num_threads_", num_threads_ );

    if ( propositionalization_ )
        {
            obj->set(
                "propositionalization_", propositionalization_->to_json_obj() );
        }

    obj->set( "shrinkage_", shrinkage_ );

    obj->set( "sampling_factor_", sampling_factor_ );

    obj->set( "round_robin_", round_robin_ );

    obj->set( "share_aggregations_", share_aggregations_ );

    obj->set( "share_conditions_", tree_hyperparameters_->share_conditions_ );

    obj->set( "share_selected_features_", share_selected_features_ );

    obj->set( "silent_", silent_ );

    obj->set( "grid_factor_", tree_hyperparameters_->grid_factor_ );

    obj->set( "regularization_", tree_hyperparameters_->regularization_ );

    obj->set( "seed_", seed_ );

    obj->set( "split_text_fields_", split_text_fields_ );

    obj->set( "vocab_size_", vocab_size_ );

    // -------------------------

    return obj;

    // -------------------------
}

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace multirel
