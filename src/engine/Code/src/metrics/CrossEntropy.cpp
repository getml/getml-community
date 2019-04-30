#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object CrossEntropy::score(
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

    std::vector<METRICS_FLOAT> cross_entropy( ncols_ );

    // -----------------------------------------------------
    // Calculate cross entropy

    for ( size_t i = 0; i < nrows_; ++i )
        {
            for ( size_t j = 0; j < ncols_; ++j )
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

    METRICS_FLOAT nrows = static_cast<METRICS_FLOAT>( nrows_ );

    // -----------------------------------------------------
    // Reduce, if necessary

    {
        std::vector<METRICS_FLOAT> global_cross_entropy( ncols_ );

        multithreading::all_reduce(
            *( comm_ ),                   // comm
            cross_entropy.data(),         // in_values
            ncols_,                       // count,
            global_cross_entropy.data(),  // out_values
            std::plus<METRICS_FLOAT>()    // op
        );

        comm().barrier();

        cross_entropy = std::move( global_cross_entropy );
    }

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
            cross_entropy[j] /= nrows;
        }

    // -----------------------------------------------------
    // If is infinite or nan, replace with -1.0.

    for ( size_t j = 0; j < ncols_; ++j )
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

    obj.set( "cross_entropy_", JSON::vector_to_array_ptr( cross_entropy ) );

    // -----------------------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------
}  // namespace metrics
