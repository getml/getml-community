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

    assert( _yhat_nrows == _y_nrows );
    assert( _yhat_ncols == _y_ncols );

    nrows_ = _yhat_nrows;
    ncols_ = _yhat_ncols;
    yhat_ = _yhat;
    y_ = _y;

    // -----------------------------------------

    std::vector<METRICS_FLOAT> mae( ncols_ );

    // -----------------------------------------------------
    // Get sum of absolute errors

    for ( size_t i = 0; i < nrows_; ++i )
        {
            for ( size_t j = 0; j < ncols_; ++j )
                {
                    mae[j] += std::abs( y( i, j ) - yhat( i, j ) );
                }
        }

    // -----------------------------------------------------
    // Get size

    METRICS_FLOAT nrows = static_cast<METRICS_FLOAT>( nrows_ );

    // -----------------------------------------------------
    // Reduce, if necessary

    if ( comm_ != nullptr )
        {
            std::vector<METRICS_FLOAT> global_mae( ncols_ );

            multithreading::all_reduce(
                *( comm_ ),                 // comm
                mae.data(),                 // in_values
                ncols_,                     // count,
                global_mae.data(),          // out_values
                std::plus<METRICS_FLOAT>()  // op
            );

            comm().barrier();

            mae = std::move( global_mae );
        }

    if ( comm_ != nullptr )
        {
            METRICS_FLOAT global_nrows;

            multithreading::all_reduce(
                *( comm_ ),                 // comm
                &nrows,                     // in_values
                1,                          // count,
                &global_nrows,              // out_values
                std::plus<METRICS_FLOAT>()  // op
            );

            comm().barrier();

            nrows = global_nrows;
        }

    // -----------------------------------------------------
    // Divide by nrows

    for ( size_t j = 0; j < ncols_; ++j )
        {
            mae[j] /= nrows;
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
