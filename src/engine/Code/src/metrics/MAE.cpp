#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object MAE::score(
    const Float* const _yhat,
    const size_t _yhat_nrows,
    const size_t _yhat_ncols,
    const Float* const _y,
    const size_t _y_nrows,
    const size_t _y_ncols )
{
    // -----------------------------------------

    impl_.set_data( _yhat, _yhat_nrows, _yhat_ncols, _y, _y_nrows, _y_ncols );

    // -----------------------------------------

    std::vector<Float> mae( ncols() );

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

    Float nrows_float = static_cast<Float>( nrows() );

    // -----------------------------------------------------
    // Reduce, if necessary

    if ( impl_.has_comm() )
        {
            impl_.reduce( std::plus<Float>(), &mae );

            impl_.reduce( std::plus<Float>(), &nrows_float );
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
