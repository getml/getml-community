#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object Scorer::get_metrics( const Poco::JSON::Object& _obj )
{
    Poco::JSON::Object result;

    if ( _obj.has( "accuracy_" ) )
        {
            result.set( "accuracy_", JSON::get_array( _obj, "accuracy_" ) );
        }

    if ( _obj.has( "auc_" ) )
        {
            result.set( "auc_", JSON::get_array( _obj, "auc_" ) );
        }

    if ( _obj.has( "cross_entropy_" ) )
        {
            result.set(
                "cross_entropy_", JSON::get_array( _obj, "cross_entropy_" ) );
        }

    if ( _obj.has( "mae_" ) )
        {
            result.set( "mae_", JSON::get_array( _obj, "mae_" ) );
        }

    if ( _obj.has( "rmse_" ) )
        {
            result.set( "rmse_", JSON::get_array( _obj, "rmse_" ) );
        }

    if ( _obj.has( "rsquared_" ) )
        {
            result.set( "rsquared_", JSON::get_array( _obj, "rsquared_" ) );
        }

    return result;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Scorer::score(
    const bool _is_classification, const Features _yhat, const Features _y )
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

            const auto scores = metric->score( _yhat, _y );

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
