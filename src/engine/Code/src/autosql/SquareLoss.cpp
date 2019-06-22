#include "autosql/lossfunctions/lossfunctions.hpp"

namespace autosql
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

SquareLoss::SquareLoss( multithreading::Communicator* _comm )
    : LossFunction(), comm_( _comm ){};

// ----------------------------------------------------------------------------

std::vector<std::vector<Float>> SquareLoss::calculate_residuals(
    const std::vector<std::vector<Float>>& _yhat_old,
    const containers::DataFrameView& _y )
{
    assert( _yhat_old.size() == _y.num_targets() );

    auto residuals = std::vector<std::vector<Float>>(
        _y.num_targets(), std::vector<Float>( _y.nrows() ) );

    for ( size_t j = 0; j < _y.num_targets(); ++j )
        {
            assert( _yhat_old[j].size() == _y.nrows() );

            for ( size_t i = 0; i < _y.nrows(); ++i )
                {
                    residuals[j][i] = _y.target( i, j ) - _yhat_old[j][i];
                }
        }

    return residuals;
}

// ----------------------------------------------------------------------------

std::vector<Float> SquareLoss::calculate_update_rates(
    const std::vector<std::vector<Float>>& _yhat_old,
    const std::vector<std::vector<Float>>& _predictions,
    const containers::DataFrameView& _y,
    const std::vector<Float>& _sample_weights )
{
    assert( _yhat_old.size() == _predictions.size() );
    assert( _yhat_old.size() == _y.num_targets() );

    std::vector<Float> update_rates( _yhat_old.size() );

    std::fill( update_rates.begin(), update_rates.end(), 1.0 );

    return update_rates;
}

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace autosql