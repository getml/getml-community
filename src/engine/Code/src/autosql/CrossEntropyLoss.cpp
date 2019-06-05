#include "lossfunctions/lossfunctions.hpp"

namespace autosql
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

CrossEntropyLoss::CrossEntropyLoss() : LossFunction(){};

// ----------------------------------------------------------------------------

containers::Matrix<SQLNET_FLOAT> CrossEntropyLoss::calculate_residuals(
    const containers::Matrix<SQLNET_FLOAT>& _yhat_old,
    const containers::DataFrameView& _y )
{
    assert( _yhat_old.nrows() == _y.nrows() );

    assert( _yhat_old.ncols() == _y.df().targets().ncols() );

    containers::Matrix<SQLNET_FLOAT> residuals(
        _y.nrows(), _y.df().targets().ncols() );

    for ( SQLNET_INT i = 0; i < _y.nrows(); ++i )
        {
            for ( SQLNET_INT j = 0; j < _y.df().targets().ncols(); ++j )
                {
                    assert( _y.targets( i, j ) == _y.targets( i, j ) );
                    assert( _yhat_old( i, j ) == _yhat_old( i, j ) );

                    residuals( i, j ) = _y.targets( i, j ) -
                                        logistic_function( _yhat_old( i, j ) );
                }
        }

    return residuals;
}

// ----------------------------------------------------------------------------

containers::Matrix<SQLNET_FLOAT> CrossEntropyLoss::calculate_update_rates(
    const containers::Matrix<SQLNET_FLOAT>& _yhat_old,
    const containers::Matrix<SQLNET_FLOAT>& _f_t,
    const containers::DataFrameView& _y,
    const containers::Matrix<SQLNET_FLOAT>& _sample_weights )
{
    // ---------------------------------------------------

    assert( _yhat_old.nrows() == _y.nrows() );
    assert( _f_t.nrows() == _y.nrows() );
    assert( _sample_weights.nrows() == _y.nrows() );

    assert( _yhat_old.ncols() == _y.df().targets().ncols() );
    assert( _f_t.ncols() == _y.df().targets().ncols() );
    assert( _sample_weights.ncols() == 1 );

    // ---------------------------------------------------
    // Calculate g_times_f and h_times_f_squared

    containers::Matrix<SQLNET_FLOAT> stats( 1, _y.df().targets().ncols() * 2 );

    SQLNET_FLOAT* g_times_f = stats.data();

    SQLNET_FLOAT* h_times_f_squared = stats.data() + _y.df().targets().ncols();

    for ( SQLNET_INT i = 0; i < _y.nrows(); ++i )
        {
            for ( SQLNET_INT j = 0; j < _y.df().targets().ncols(); ++j )
                {
                    const auto logistic =
                        logistic_function( _yhat_old( i, j ) );

                    //  ( 1.0 / ( exp( -yhat ) + 1.0 ) - y ) * f_t
                    g_times_f[j] +=
                        ( logistic - _y.targets( i, j ) ) * _f_t( i, j );

                    //  logistic * (1.0 - logistic) * f_t * f_t
                    // The derivative of the logistic function =
                    // logistic * (1.0 - logistic)
                    h_times_f_squared[j] += logistic * ( 1.0 - logistic ) *
                                            _f_t( i, j ) * _f_t( i, j );
                }
        }

        // ---------------------------------------------------
        // Get global sums, if necessary

#ifdef SQLNET_PARALLEL

    containers::Matrix<SQLNET_FLOAT> global_stats(
        1, _y.df().targets().ncols() * 2 );

    SQLNET_PARALLEL_LIB::all_reduce(
        comm(),
        stats.data(),
        stats.ncols(),
        global_stats.data(),
        std::plus<SQLNET_FLOAT>() );

    comm().barrier();

    stats = global_stats;

#endif  // SQLNET_PARALLEL

    // ---------------------------------------------------
    // Calculate update_rates

    containers::Matrix<SQLNET_FLOAT> update_rates(
        1, _y.df().targets().ncols() );

    for ( SQLNET_INT j = 0; j < _y.df().targets().ncols(); ++j )
        {
            update_rates[j] = ( -1.0 ) * g_times_f[j] / h_times_f_squared[j];
        }

    // ---------------------------------------------------
    // Make sure that update_rates are in range

    auto in_range = []( SQLNET_FLOAT& val ) {
        val = ( ( std::isnan( val ) || std::isinf( val ) ) ? 0.0 : val );
    };

    std::for_each( update_rates.begin(), update_rates.end(), in_range );

    // ---------------------------------------------------

    return update_rates;
}

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace autosql