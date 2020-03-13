#ifndef MULTIREL_TIMESERIES_HYPERPARAMETERS_HPP_
#define MULTIREL_TIMESERIES_HYPERPARAMETERS_HPP_

namespace multirel
{
namespace timeseries
{
// ----------------------------------------------------------------------------

struct Hyperparameters
{
    // ------------------------------------------------------

    Hyperparameters( const Poco::JSON::Object& _json_obj )
        : lag_( JSON::get_value<Float>( _json_obj, "lag_" ) ),
          memory_( JSON::get_value<Float>( _json_obj, "memory_" ) ),
          multirel_hyperparameters_(
              descriptors::Hyperparameters( _json_obj ) ),
          ts_name_( JSON::get_value<std::string>( _json_obj, "ts_name_" ) )
    {
    }

    ~Hyperparameters() = default;

    // ------------------------------------------------------

    /// The lag used for the time series prediction.
    const Float lag_;

    /// The length of the memory used for the time series prediction.
    const Float memory_;

    /// The hyperparameters for the underlying MultirelModel.
    const descriptors::Hyperparameters multirel_hyperparameters_;

    /// The name of the time stamp used for the time series.
    const std::string ts_name_;
};

// ----------------------------------------------------------------------------
}  // namespace timeseries
}  // namespace multirel

#endif  // MULTIREL_TIMESERIES_HYPERPARAMETERS_HPP_
