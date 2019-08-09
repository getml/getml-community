#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object CrossEntropy::score(
    const Features _yhat, const Features _y )
{
    // -----------------------------------------

    impl_.set_data( _yhat, _y );

    // -----------------------------------------

    std::vector<Float> cross_entropy( ncols() );

    // -----------------------------------------------------
    // Calculate cross entropy

    for ( size_t i = 0; i < nrows(); ++i )
        {
            for ( size_t j = 0; j < ncols(); ++j )
                {
                    if ( y( i, j ) == 0.0 )
                        {
                            cross_entropy[j] -= std::log( 1.0 - yhat( i, j ) );
                        }
                    else if ( y( i, j ) == 1.0 )
                        {
                            cross_entropy[j] -= std::log( yhat( i, j ) );
                        }
                    else
                        {
                            throw std::invalid_argument(
                                "Target must either be 0 or 1 for "
                                "cross entropy score to work!" );
                        }
                }
        }

    // -----------------------------------------------------
    // Get nrows

    Float nrows_float = static_cast<Float>( nrows() );

    // -----------------------------------------------------
    // Reduce, if necessary

    if ( impl_.has_comm() )
        {
            impl_.reduce( std::plus<Float>(), &cross_entropy );

            impl_.reduce( std::plus<Float>(), &nrows_float );
        }

    // -----------------------------------------------------
    // Divide by nrows

    for ( size_t j = 0; j < ncols(); ++j )
        {
            cross_entropy[j] /= nrows_float;
        }

    // -----------------------------------------------------
    // If is infinite or nan, replace with -1.0.

    for ( size_t j = 0; j < ncols(); ++j )
        {
            if ( std::isinf( cross_entropy[j] ) ||
                 std::isnan( cross_entropy[j] ) )
                {
                    cross_entropy[j] = -1.0;
                }
        }

    // -----------------------------------------------------
    // Transform to object and return.

    Poco::JSON::Object obj;

    obj.set(
        "cross_entropy_",
        jsonutils::JSON::vector_to_array_ptr( cross_entropy ) );

    // -----------------------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------
}  // namespace metrics
