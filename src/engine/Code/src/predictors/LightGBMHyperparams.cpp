#include "predictors/predictors.hpp"

namespace predictors
{
// ----------------------------------------------------------------------------

LightGBMHyperparams::LightGBMHyperparams( const Poco::JSON::Object &_json_obj )
    : boosting_type_(
          JSON::get_value<std::string>( _json_obj, "boosting_type_" ) ),
      colsample_bytree_(
          JSON::get_value<Float>( _json_obj, "colsample_bytree_" ) ),
      learning_rate_( JSON::get_value<Float>( _json_obj, "learning_rate_" ) ),
      max_depth_( JSON::get_value<Int>( _json_obj, "max_depth_" ) ),
      min_child_samples_(
          JSON::get_value<Int>( _json_obj, "min_child_samples_" ) ),
      min_child_weight_(
          JSON::get_value<Float>( _json_obj, "min_child_weight_" ) ),
      min_split_gain_( JSON::get_value<Float>( _json_obj, "min_split_gain_" ) ),
      n_estimators_( JSON::get_value<Int>( _json_obj, "n_estimators_" ) ),
      num_leaves_( JSON::get_value<Int>( _json_obj, "num_leaves_" ) ),
      n_jobs_( JSON::get_value<Int>( _json_obj, "n_jobs_" ) ),
      objective_( JSON::get_value<std::string>( _json_obj, "objective_" ) ),
      random_state_( JSON::get_value<Int>( _json_obj, "random_state_" ) ),
      reg_alpha_( JSON::get_value<Float>( _json_obj, "reg_alpha_" ) ),
      reg_lambda_( JSON::get_value<Float>( _json_obj, "reg_lambda_" ) ),
      silent_( JSON::get_value<bool>( _json_obj, "silent_" ) ),
      subsample_( JSON::get_value<Float>( _json_obj, "subsample_" ) ),
      subsample_for_bin_(
          JSON::get_value<Int>( _json_obj, "subsample_for_bin_" ) ),
      subsample_freq_( JSON::get_value<Int>( _json_obj, "subsample_freq_" ) )
{
    std::cout << "Created LightGBMHyperparams" << std::endl;
}

// ------------------------------------------------------------------------
}  // namespace predictors
