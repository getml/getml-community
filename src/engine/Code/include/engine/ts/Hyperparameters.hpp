#ifndef ENGINE_TS_HYPERPARAMETERS_HPP_
#define ENGINE_TS_HYPERPARAMETERS_HPP_

namespace engine
{
namespace ts
{
// ----------------------------------------------------------------------------

template <class HypType>
struct Hyperparameters
{
    // ------------------------------------------------------

    Hyperparameters( const Poco::JSON::Object& _json_obj )
        : allow_lagged_targets_( jsonutils::JSON::get_value<bool>(
              _json_obj, "allow_lagged_targets_" ) ),
          horizon_(
              jsonutils::JSON::get_value<Float>( _json_obj, "horizon_" ) ),
          memory_( jsonutils::JSON::get_value<Float>( _json_obj, "memory_" ) ),
          model_hyperparams_( std::make_shared<HypType>( _json_obj ) ),
          self_join_keys_( jsonutils::JSON::array_to_vector<std::string>(
              jsonutils::JSON::get_array( _json_obj, "self_join_keys_" ) ) ),
          silent_( model_hyperparams_->silent_ ),
          ts_name_(
              jsonutils::JSON::get_value<std::string>( _json_obj, "ts_name_" ) )
    {
        if ( horizon_ < 0.0 )
            {
                throw std::invalid_argument(
                    "'horizon' must be greater or equal to 0!" );
            }

        if ( memory_ < 0.0 )
            {
                throw std::invalid_argument(
                    "'memory' must be greater or equal to 0!" );
            }

        if ( allow_lagged_targets_ && horizon_ == 0.0 )
            {
                throw std::invalid_argument(
                    "if you are allowing lagged targets, then the horizon "
                    "cannot be 0!" );
            }
    }

    ~Hyperparameters() = default;

    // ------------------------------------------------------

    /// Whether we want to allow a lagged version of the targets to be included
    /// in the peripheral table.
    const bool allow_lagged_targets_;

    /// The forecast horizon used for the time series prediction.
    const Float horizon_;

    /// The length of the memory used for the time series prediction.
    const Float memory_;

    /// The hyperparameters for the underlying feature learner.
    const std::shared_ptr<const HypType> model_hyperparams_;

    /// The join keys used for the self join.
    const std::vector<std::string> self_join_keys_;

    /// Whether we want the time series to be silent.
    const bool silent_;

    /// The name of the time stamp used for the time series.
    const std::string ts_name_;
};

// ----------------------------------------------------------------------------
}  // namespace ts
}  // namespace engine

#endif  // ENGINE_TS_HYPERPARAMETERS_HPP_
