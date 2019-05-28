#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object Scorer::score(
    const bool _is_classification,
    const METRICS_FLOAT* const _yhat,
    const size_t _yhat_nrows,
    const size_t _yhat_ncols,
    const METRICS_FLOAT* const _y,
    const size_t _y_nrows,
    const size_t _y_ncols )
{
    // ------------------------------------------------------------------------
    // Build up names.

    std::vector<std::string> names;

    if ( _is_classification )
        {
            names.push_back( "accuracy_" );
            names.push_back( "auc_" );
            names.push_back( "cross_entropy_" );
        }
    else
        {
            names.push_back( "mae_" );
            names.push_back( "rmse_" );
            names.push_back( "rsquared_" );
        }

    // ------------------------------------------------------------------------
    // Do the actual scoring.

    Poco::JSON::Object obj;

    for ( const auto& name : names )
        {
            // TODO: Replace nullptr with &comm() after multithreading is
            // implemented.
            const auto metric = metrics::MetricParser::parse( name, nullptr );

            const auto scores = metric->score(
                _yhat, _yhat_nrows, _yhat_ncols, _y, _y_nrows, _y_ncols );

            for ( auto it = scores.begin(); it != scores.end(); ++it )
                {
                    obj.set( it->first, it->second );
                }
        }

    // ------------------------------------------------------------------------

    return obj;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace metrics
