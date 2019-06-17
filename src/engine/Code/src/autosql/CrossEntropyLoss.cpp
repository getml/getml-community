#include "autosql/lossfunctions/lossfunctions.hpp"

namespace autosql
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

CrossEntropyLoss::CrossEntropyLoss( multithreading::Communicator* _comm )
    : LossFunction(), comm_( _comm ){};

// ----------------------------------------------------------------------------

std::vector<std::vector<AUTOSQL_FLOAT>> CrossEntropyLoss::calculate_residuals(
    const std::vector<std::vector<AUTOSQL_FLOAT>>& _yhat_old,
    const containers::DataFrameView& _y )
{
    assert( _yhat_old.size() == _y.num_targets() );

    auto residuals = std::vector<std::vector<AUTOSQL_FLOAT>>(
        _y.num_targets(), std::vector<AUTOSQL_FLOAT>( _y.nrows() ) );

    for ( size_t j = 0; j < _y.num_targets(); ++j )
        {
            assert( _yhat_old[j].size() == _y.nrows() );

            for ( size_t i = 0; i < _y.nrows(); ++i )
                {
                    assert( !std::isnan( _y.target( i, j ) ) );
                    assert( !std::isnan( _yhat_old[j][i] ) );

                    residuals[j][i] = _y.target( i, j ) -
                                      logistic_function( _yhat_old[j][i] );
                }
        }

    return residuals;
}

// ----------------------------------------------------------------------------

std::vector<AUTOSQL_FLOAT> CrossEntropyLoss::calculate_update_rates(
    const std::vector<std::vector<AUTOSQL_FLOAT>>& _yhat_old,
    const std::vector<std::vector<AUTOSQL_FLOAT>>& _predictions,
    const containers::DataFrameView& _y,
    const std::vector<AUTOSQL_FLOAT>& _sample_weights )
{
    // ---------------------------------------------------

    assert( _yhat_old.size() == _predictions.size() );
    assert( _yhat_old.size() == _y.num_targets() );

    // ---------------------------------------------------
    // Calculate g_times_f and h_times_f_squared

    auto stats = std::vector<AUTOSQL_FLOAT>( _y.num_targets() * 2 );

    AUTOSQL_FLOAT* g_times_p = stats.data();

    AUTOSQL_FLOAT* h_times_p_squared = stats.data() + _y.num_targets();

    for ( size_t j = 0; j < _y.num_targets(); ++j )
        {
            assert( _yhat_old[j].size() == _y.nrows() );
            assert( _predictions[j].size() == _y.nrows() );

            for ( size_t i = 0; i < _y.nrows(); ++i )
                {
                    const auto logistic = logistic_function( _yhat_old[j][i] );

                    g_times_p[j] +=
                        ( logistic - _y.target( i, j ) ) * _predictions[j][i];

                    h_times_p_squared[j] += logistic * ( 1.0 - logistic ) *
                                            _predictions[j][i] *
                                            _predictions[j][i];
                }
        }

    utils::Reducer::reduce( std::plus<AUTOSQL_FLOAT>(), &stats, comm_ );

    // ---------------------------------------------------
    // Calculate update_rates

    std::vector<AUTOSQL_FLOAT> update_rates( _y.num_targets() );

    for ( size_t j = 0; j < _y.num_targets(); ++j )
        {
            update_rates[j] = ( -1.0 ) * g_times_p[j] / h_times_p_squared[j];
        }

    // ---------------------------------------------------
    // Make sure that update_rates are in range

    auto in_range = []( AUTOSQL_FLOAT& val ) {
        val = ( ( std::isnan( val ) || std::isinf( val ) ) ? 0.0 : val );
    };

    std::for_each( update_rates.begin(), update_rates.end(), in_range );

    // ---------------------------------------------------

    return update_rates;

    // ---------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace autosql