#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object RMSE::score( const Features _yhat, const Features _y )
{
    // -----------------------------------------

    impl_.set_data( _yhat, _y );

    // -----------------------------------------

    std::vector<Float> rmse( ncols() );

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

    Float nrows_float = static_cast<Float>( nrows() );

    // -----------------------------------------------------
    // Reduce, if necessary

    if ( impl_.has_comm() )
        {
            impl_.reduce( std::plus<Float>(), &rmse );

            impl_.reduce( std::plus<Float>(), &nrows_float );
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
