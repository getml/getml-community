#include "predictors/predictors.hpp"

namespace predictors
{
// ----------------------------------------------------------------------------

XGBoostHyperparams::XGBoostHyperparams( const Poco::JSON::Object &_json_obj )
    : alpha_( JSON::get_value<Float>( _json_obj, "reg_alpha_" ) ),
      booster_( JSON::get_value<std::string>( _json_obj, "booster_" ) ),
      colsample_bylevel_(
          JSON::get_value<Float>( _json_obj, "colsample_bylevel_" ) ),
      colsample_bytree_(
          JSON::get_value<Float>( _json_obj, "colsample_bytree_" ) ),
      early_stopping_rounds_(
          JSON::get_value<size_t>( _json_obj, "early_stopping_rounds_" ) ),
      eta_( JSON::get_value<Float>( _json_obj, "learning_rate_" ) ),
      gamma_( JSON::get_value<Float>( _json_obj, "gamma_" ) ),
      lambda_( JSON::get_value<Float>( _json_obj, "reg_lambda_" ) ),
      max_delta_step_( JSON::get_value<Float>( _json_obj, "max_delta_step_" ) ),
      max_depth_( JSON::get_value<size_t>( _json_obj, "max_depth_" ) ),
      min_child_weights_(
          JSON::get_value<Float>( _json_obj, "min_child_weights_" ) ),
      n_iter_( JSON::get_value<size_t>( _json_obj, "n_estimators_" ) ),
      normalize_type_(
          JSON::get_value<std::string>( _json_obj, "normalize_type_" ) ),
      num_parallel_tree_(
          JSON::get_value<size_t>( _json_obj, "num_parallel_tree_" ) ),
      nthread_( JSON::get_value<Int>( _json_obj, "n_jobs_" ) ),
      objective_( JSON::get_value<std::string>( _json_obj, "objective_" ) ),
      one_drop_( JSON::get_value<bool>( _json_obj, "one_drop_" ) ),
      rate_drop_( JSON::get_value<Float>( _json_obj, "rate_drop_" ) ),
      sample_type_( JSON::get_value<std::string>( _json_obj, "sample_type_" ) ),
      silent_( JSON::get_value<bool>( _json_obj, "silent_" ) ),
      skip_drop_( JSON::get_value<Float>( _json_obj, "skip_drop_" ) ),
      subsample_( JSON::get_value<Float>( _json_obj, "subsample_" ) )
{
    if ( booster_ != "gbtree" && booster_ != "gblinear" && booster_ != "dart" )
        {
            throw std::invalid_argument(
                "Booster of type '" + booster_ +
                "' not known! Please use 'gbtree', 'gblinear' or 'dart'!" );
        }

    if ( objective_ != "reg:linear" && objective_ != "reg:squarederror" &&
         objective_ != "reg:logistic" && objective_ != "binary:logistic" &&
         objective_ != "binary:logitraw" && objective_ != "reg:tweedie" )
        {
            throw std::invalid_argument(
                "Objective of type '" + objective_ +
                "' not known! Please use 'reg:squarederror', 'reg:linear', "
                "'reg:logistic', "
                "'binary:logistic', 'binary:logitraw' or 'reg:tweedie'!" );
        }

    if ( normalize_type_ != "tree" && normalize_type_ != "forest" )
        {
            throw std::invalid_argument(
                "Normalize_type of type '" + normalize_type_ +
                "' not known! Please use 'tree' or 'forest'!" );
        }

    if ( sample_type_ != "uniform" && sample_type_ != "weighted" )
        {
            throw std::invalid_argument(
                "Sample_type of type '" + sample_type_ +
                "' not known! Please use 'uniform' or 'weighted'!" );
        }
}

// ------------------------------------------------------------------------
}  // namespace predictors
