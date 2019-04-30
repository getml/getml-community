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

    assert( _yhat_nrows == _y_nrows );
    assert( _yhat_ncols == _y_ncols );

    nrows_ = _yhat_nrows;
    ncols_ = _yhat_ncols;
    yhat_ = _yhat;
    y_ = _y;

    // -----------------------------------------

    std::vector<METRICS_FLOAT> rmse( ncols_ );

    // -----------------------------------------------------
    // Get sum of squared errors

    for ( size_t i = 0; i < nrows_; ++i )
        {
            for ( size_t j = 0; j < ncols_; ++j )
                {
                    rmse[j] += ( y( i, j ) - yhat( i, j ) ) *
                               ( y( i, j ) - yhat( i, j ) );
                }
        }

    // -----------------------------------------------------
    // Get nrows

    METRICS_FLOAT nrows = static_cast<METRICS_FLOAT>( nrows_ );

    // -----------------------------------------------------
    // Reduce, if necessary

    {
        std::vector<METRICS_FLOAT> global_rmse( ncols_ );

        multithreading::all_reduce(
            *( comm_ ),                  // comm
            rmse.data(),                 // in_values
            ncols_,                      // count,
            global_rmse.data(),          // out_values
            std::plus<METRICS_FLOAT>()  // op
        );

        comm().barrier();

        rmse = std::move( global_rmse );
    }

    {
        METRICS_FLOAT global_nrows;

        multithreading::all_reduce(
            *( comm_ ),                  // comm
            &nrows,                      // in_values
            1,                           // count,
            &global_nrows,               // out_values
            std::plus<METRICS_FLOAT>()  // op
        );

        comm().barrier();

        nrows = global_nrows;
    }

    // -----------------------------------------------------
    // Divide by nrows and get square root

    for ( size_t j = 0; j < ncols_; ++j )
        {
            rmse[j] /= nrows;

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
