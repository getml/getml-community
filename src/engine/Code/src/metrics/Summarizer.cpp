#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object Summarizer::calculate_feature_correlations(
    const std::vector<METRICS_FLOAT>& _features,
    const size_t _nrows,
    const size_t _ncols,
    const std::vector<const METRICS_FLOAT*>& _targets )
{
    // -----------------------------------------------------------

    const auto t_ncols = _targets.size();

    // -----------------------------------------------------------
    // Prepare datasets

    std::vector<METRICS_FLOAT> sum_yhat( _ncols );

    std::vector<METRICS_FLOAT> sum_yhat_yhat( _ncols );

    std::vector<METRICS_FLOAT> sum_y( t_ncols );

    std::vector<METRICS_FLOAT> sum_y_y( t_ncols );

    std::vector<METRICS_FLOAT> sum_yhat_y( _ncols * t_ncols );

    // -----------------------------------------------------------
    // Calculate sufficient statistics

    // Calculate sum yhat
    for ( size_t i = 0; i < _nrows; ++i )
        for ( size_t j = 0; j < _ncols; ++j )
            sum_yhat[j] += get( i, j, _ncols, _features );

    // Calculate sum yhat_yhat
    for ( size_t i = 0; i < _nrows; ++i )
        for ( size_t j = 0; j < _ncols; ++j )
            sum_yhat_yhat[j] +=
                get( i, j, _ncols, _features ) * get( i, j, _ncols, _features );

    // Calculate sum y
    for ( size_t k = 0; k < t_ncols; ++k )
        for ( size_t i = 0; i < _nrows; ++i ) sum_y[k] += _targets[k][i];

    // Calculate sum y_y
    for ( size_t k = 0; k < t_ncols; ++k )
        for ( size_t i = 0; i < _nrows; ++i )
            sum_y_y[k] += _targets[k][i] * _targets[k][i];

    // Calculate sum_yhat_y
    for ( size_t k = 0; k < t_ncols; ++k )
        for ( size_t i = 0; i < _nrows; ++i )
            for ( size_t j = 0; j < _ncols; ++j )
                get( j, k, t_ncols, &sum_yhat_y ) +=
                    get( i, j, _ncols, _features ) * _targets[k][i];

    // -----------------------------------------------------------
    // Calculate correlations from sufficient statistics

    const auto n = static_cast<METRICS_FLOAT>( _nrows );

    auto feature_correlations = std::vector<std::vector<METRICS_FLOAT>>(
        _ncols, std::vector<METRICS_FLOAT>( t_ncols ) );

    for ( size_t j = 0; j < _ncols; ++j )
        for ( size_t k = 0; k < t_ncols; ++k )
            {
                const METRICS_FLOAT var_yhat =
                    sum_yhat_yhat[j] / n -
                    ( sum_yhat[j] / n ) * ( sum_yhat[j] / n );

                const METRICS_FLOAT var_y =
                    sum_y_y[k] / n - ( sum_y[k] / n ) * ( sum_y[k] / n );

                const METRICS_FLOAT cov_y_yhat =
                    get( j, k, t_ncols, &sum_yhat_y ) / n -
                    ( sum_yhat[j] / n ) * ( sum_y[k] / n );

                feature_correlations[j][k] =
                    cov_y_yhat / sqrt( var_yhat * var_y );

                if ( feature_correlations[j][k] != feature_correlations[j][k] )
                    {
                        feature_correlations[j][k] = 0.0;
                    }
            }

    // -----------------------------------------------------------
    // Transform to JSON Array.

    Poco::JSON::Array::Ptr arr( new Poco::JSON::Array() );

    for ( const auto& f : feature_correlations )
        {
            arr->add( JSON::vector_to_array_ptr( f ) );
        }

    // -----------------------------------------------------------
    // Add to JSON object.

    Poco::JSON::Object obj;

    obj.set( "feature_correlations_", arr );

    return obj;

    // -----------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace metrics
