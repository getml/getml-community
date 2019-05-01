#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object MAE::score(
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

    std::vector<METRICS_FLOAT> mae( ncols() );

    // -----------------------------------------------------
    // Get sum of absolute errors

    for ( size_t i = 0; i < nrows(); ++i )
        {
            for ( size_t j = 0; j < ncols(); ++j )
                {
                    mae[j] += std::abs( y( i, j ) - yhat( i, j ) );
                }
        }

    // -----------------------------------------------------
    // Get size

    METRICS_FLOAT nrows_float = static_cast<METRICS_FLOAT>( nrows() );

    // -----------------------------------------------------
    // Reduce, if necessary

    if ( impl_.has_comm() )
        {
            impl_.reduce( std::plus<METRICS_FLOAT>(), &mae );

            impl_.reduce( std::plus<METRICS_FLOAT>(), &nrows_float );
        }

    // -----------------------------------------------------
    // Divide by nrows

    for ( size_t j = 0; j < ncols(); ++j )
        {
            mae[j] /= nrows_float;
        }

    // -----------------------------------------------------
    // Transform to object and return.

    Poco::JSON::Object obj;

    obj.set( "mae_", JSON::vector_to_array_ptr( mae ) );

    // -----------------------------------------------------

    return obj;

    // -----------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace metrics
