#ifndef TS_HYPERPARAMETERS_HPP_
#define TS_HYPERPARAMETERS_HPP_

namespace ts
{
// ----------------------------------------------------------------------------

template <class HypType>
struct Hyperparameters
{
    // ------------------------------------------------------

    Hyperparameters( const Poco::JSON::Object& _json_obj )
        : lag_( jsonutils::JSON::get_value<Float>( _json_obj, "lag_" ) ),
          memory_( jsonutils::JSON::get_value<Float>( _json_obj, "memory_" ) ),
          model_hyperparams_( std::make_shared<HypType>( _json_obj ) ),
          self_join_keys_( jsonutils::JSON::array_to_vector<std::string>(
              jsonutils::JSON::get_array( _json_obj, "self_join_keys_" ) ) ),
          ts_name_(
              jsonutils::JSON::get_value<std::string>( _json_obj, "ts_name_" ) )
    {
    }

    ~Hyperparameters() = default;

    // ------------------------------------------------------

    /// The lag used for the time series prediction.
    const Float lag_;

    /// The length of the memory used for the time series prediction.
    const Float memory_;

    /// The hyperparameters for the underlying feature engineerer.
    const std::shared_ptr<const HypType> model_hyperparams_;

    /// The join keys used for the self join.
    const std::vector<std::string> self_join_keys_;

    /// The name of the time stamp used for the time series.
    const std::string ts_name_;
};

// ----------------------------------------------------------------------------
}  // namespace ts

#endif  // TS_HYPERPARAMETERS_HPP_
