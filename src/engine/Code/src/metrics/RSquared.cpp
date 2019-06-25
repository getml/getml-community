#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object RSquared::score( const Features _yhat, const Features _y )
{
    // -----------------------------------------------------

    impl_.set_data( _yhat, _y );

    sufficient_statistics_ = std::vector<Float>( 6 * ncols() );

    // -----------------------------------------------------
    // Calculate sufficient statistics

    // Calculate sum yhat
    for ( size_t i = 0; i < nrows(); ++i )
        for ( size_t j = 0; j < ncols(); ++j )
            sufficient_statistics( 0, j ) += yhat( i, j );

    // Calculate sum yhat squared
    for ( size_t i = 0; i < nrows(); ++i )
        for ( size_t j = 0; j < ncols(); ++j )
            sufficient_statistics( 1, j ) += yhat( i, j ) * yhat( i, j );

    // Calculate sum y
    for ( size_t i = 0; i < nrows(); ++i )
        for ( size_t j = 0; j < ncols(); ++j )
            sufficient_statistics( 2, j ) += y( i, j );

    // Calculate sum y squared
    for ( size_t i = 0; i < nrows(); ++i )
        for ( size_t j = 0; j < ncols(); ++j )
            sufficient_statistics( 3, j ) += y( i, j ) * y( i, j );

    // Calculate sum yhat*y
    for ( size_t i = 0; i < nrows(); ++i )
        for ( size_t j = 0; j < ncols(); ++j )
            sufficient_statistics( 4, j ) += yhat( i, j ) * y( i, j );

    // Store n
    sufficient_statistics( 5, 0 ) = static_cast<Float>( nrows() );

    // -----------------------------------------------------
    // Reduce, if necessary

    if ( impl_.has_comm() )
        {
            impl_.reduce( std::plus<Float>(), &sufficient_statistics_ );
        }

    // -----------------------------------------------------
    // Calculate rsquared

    std::vector<Float> rsquared( ncols() );

    for ( size_t j = 0; j < ncols(); ++j )
        {
            const Float sum_yhat = sufficient_statistics( 0, j );

            const Float sum_yhat_yhat = sufficient_statistics( 1, j );

            const Float sum_y = sufficient_statistics( 2, j );

            const Float sum_y_y = sufficient_statistics( 3, j );

            const Float sum_yhat_y = sufficient_statistics( 4, j );

            // n is the same for all j!
            const Float n = sufficient_statistics( 5, 0 );

            const Float var_yhat =
                sum_yhat_yhat / n - ( sum_yhat / n ) * ( sum_yhat / n );

            const Float var_y = sum_y_y / n - ( sum_y / n ) * ( sum_y / n );

            const Float cov_y_yhat =
                sum_yhat_y / n - ( sum_yhat / n ) * ( sum_y / n );

            rsquared[j] = ( cov_y_yhat * cov_y_yhat ) / ( var_yhat * var_y );
        }

    // -----------------------------------------------------
    // If is infinite or nan, replace with -1.0.

    for ( size_t j = 0; j < ncols(); ++j )
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
