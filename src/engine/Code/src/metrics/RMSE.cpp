#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object RMSE::score(
    const METRICS_FLOAT* const _yhat,
    const size_t _yhat_nrows,
    const size_t _yhat_ncols,
    const METRICS_FLOAT* const _y,
    const size_t _y_nrows,
    const size_t _y_ncols )
{
    // -----------------------------------------

    impl_.set_data( _yhat, _yhat_nrows, _yhat_ncols, _y, _y_nrows, _y_ncols );

    // -----------------------------------------

    std::vector<METRICS_FLOAT> rmse( ncols() );

    // -----------------------------------------------------
    // Get sum of squared errors

    for ( size_t i = 0; i < nrows(); ++i )
        {
            for ( size_t j = 0; j < ncols(); ++j )
                {
                    rmse[j] += ( y( i, j ) - yhat( i, j ) ) *
                               ( y( i, j ) - yhat( i, j ) );
                }
        }

    // -----------------------------------------------------
    // Get nrows

    METRICS_FLOAT nrows_float = static_cast<METRICS_FLOAT>( nrows() );

    // -----------------------------------------------------
    // Reduce, if necessary

    if ( impl_.has_comm() )
        {
            impl_.reduce( std::plus<METRICS_FLOAT>(), &rmse );

            impl_.reduce( std::plus<METRICS_FLOAT>(), &nrows_float );
        }

    // -----------------------------------------------------
    // Divide by nrows and get square root

    for ( size_t j = 0; j < ncols(); ++j )
        {
            rmse[j] /= nrows_float;

            rmse[j] = std::sqrt( rmse[j] );
        }

    // -----------------------------------------------------
    // Transform to object and return.

    Poco::JSON::Object obj;

    obj.set( "rmse_", JSON::vector_to_array_ptr( rmse ) );

    // -----------------------------------------------------

    return obj;

    // -----------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace metrics
