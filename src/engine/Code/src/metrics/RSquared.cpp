#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object RSquared::score(
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

    sufficient_statistics_ = std::vector<METRICS_FLOAT>( 6 * ncols_ );

    // -----------------------------------------------------
    // Calculate sufficient statistics

    // Calculate sum yhat
    for ( size_t i = 0; i < nrows_; ++i )
        for ( size_t j = 0; j < ncols_; ++j )
            sufficient_statistics( 0, j ) += yhat( i, j );

    // Calculate sum yhat squared
    for ( size_t i = 0; i < nrows_; ++i )
        for ( size_t j = 0; j < ncols_; ++j )
            sufficient_statistics( 1, j ) += yhat( i, j ) * yhat( i, j );

    // Calculate sum yhat
    for ( size_t i = 0; i < nrows_; ++i )
        for ( size_t j = 0; j < ncols_; ++j )
            sufficient_statistics( 2, j ) += y( i, j );

    // Calculate sum yhat squared
    for ( size_t i = 0; i < nrows_; ++i )
        for ( size_t j = 0; j < ncols_; ++j )
            sufficient_statistics( 3, j ) += y( i, j ) * y( i, j );

    // Calculate sum yhat*y
    for ( size_t i = 0; i < nrows_; ++i )
        for ( size_t j = 0; j < ncols_; ++j )
            sufficient_statistics( 4, j ) += yhat( i, j ) * y( i, j );

    // Store n
    sufficient_statistics( 5, 0 ) = static_cast<METRICS_FLOAT>( nrows_ );

    // -----------------------------------------------------
    // Reduce, if necessary

    {
        std::vector<METRICS_FLOAT> global( sufficient_statistics_.size() );

        multithreading::all_reduce(
            *comm_,                         // comm
            sufficient_statistics_.data(),  // in_values
            sufficient_statistics_.size(),  // count,
            global.data(),                  // out_values
            std::plus<METRICS_FLOAT>()      // op
        );

        comm().barrier();

        sufficient_statistics_ = std::move( global );
    }

    // -----------------------------------------------------
    // Calculate rsquared

    std::vector<METRICS_FLOAT> rsquared( ncols_ );

    for ( size_t j = 0; j < ncols_; ++j )
        {
            const METRICS_FLOAT sum_yhat = sufficient_statistics( 0, j );

            const METRICS_FLOAT sum_yhat_yhat = sufficient_statistics( 1, j );

            const METRICS_FLOAT sum_y = sufficient_statistics( 2, j );

            const METRICS_FLOAT sum_y_y = sufficient_statistics( 3, j );

            const METRICS_FLOAT sum_yhat_y = sufficient_statistics( 4, j );

            // n is the same for all j!
            const METRICS_FLOAT n = sufficient_statistics( 5, 0 );

            const METRICS_FLOAT var_yhat =
                sum_yhat_yhat / n - ( sum_yhat / n ) * ( sum_yhat / n );

            const METRICS_FLOAT var_y =
                sum_y_y / n - ( sum_y / n ) * ( sum_y / n );

            const METRICS_FLOAT cov_y_yhat =
                sum_yhat_y / n - ( sum_yhat / n ) * ( sum_y / n );

            rsquared[j] = ( cov_y_yhat * cov_y_yhat ) / ( var_yhat * var_y );
        }

    // -----------------------------------------------------
    // If is infinite or nan, replace with -1.0.

    for ( size_t j = 0; j < ncols_; ++j )
        {
            if ( std::isinf( rsquared[j] ) || std::isnan( rsquared[j] ) )
                {
                    rsquared[j] = -1.0;
                }
        }

    // -----------------------------------------------------
    // Transform to object and return.

    Poco::JSON::Object obj;

    obj.set( "rsquared_", JSON::vector_to_array_ptr( rsquared ) );

    // -----------------------------------------------------

    sufficient_statistics_.clear();

    // -----------------------------------------------------

    return obj;

    // -----------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace metrics
