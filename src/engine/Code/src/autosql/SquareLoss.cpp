#include "lossfunctions/lossfunctions.hpp"

namespace autosql
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

SquareLoss::SquareLoss() : LossFunction(){};

// ----------------------------------------------------------------------------

containers::Matrix<AUTOSQL_FLOAT> SquareLoss::calculate_residuals(
    const containers::Matrix<AUTOSQL_FLOAT>& _yhat_old,
    const containers::DataFrameView& _y )
{
    assert( _yhat_old.nrows() == _y.nrows() );

    assert( _yhat_old.ncols() == _y.df().targets().ncols() );

    containers::Matrix<AUTOSQL_FLOAT> residuals(
        _y.nrows(), _y.df().targets().ncols() );

    for ( AUTOSQL_INT i = 0; i < _y.nrows(); ++i )
        {
            for ( AUTOSQL_INT j = 0; j < _y.df().targets().ncols(); ++j )
                {
                    residuals( i, j ) = _y.targets( i, j ) - _yhat_old( i, j );
                }
        }

    return residuals;
}

// ----------------------------------------------------------------------------

containers::Matrix<AUTOSQL_FLOAT> SquareLoss::calculate_update_rates(
    const containers::Matrix<AUTOSQL_FLOAT>& _yhat_old,
    const containers::Matrix<AUTOSQL_FLOAT>& _f_t,
    const containers::DataFrameView& _y,
    const containers::Matrix<AUTOSQL_FLOAT>& _sample_weights )
{
    assert( _yhat_old.nrows() == _y.nrows() );
    assert( _f_t.nrows() == _y.nrows() );
    assert( _sample_weights.nrows() == _y.nrows() );

    assert( _yhat_old.ncols() == _y.df().targets().ncols() );
    assert( _f_t.ncols() == _y.df().targets().ncols() );
    assert( _sample_weights.ncols() == 1 );

    containers::Matrix<AUTOSQL_FLOAT> update_rates( 1, _f_t.ncols() );

    std::fill( update_rates.begin(), update_rates.end(), 1.0 );

    return update_rates;
}

// ----------------------------------------------------------------------------
}
}