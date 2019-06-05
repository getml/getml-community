#include "LinearRegression.hpp"

namespace autosql
{
// ----------------------------------------------------------------------------

LinearRegression::LinearRegression(){};

// ----------------------------------------------------------------------------

LinearRegression::LinearRegression( AUTOSQL_INT _ncols )
{
    assert( _ncols > 0 );

    slopes_ = containers::Matrix<AUTOSQL_FLOAT>( 1, _ncols );

    intercepts_ = containers::Matrix<AUTOSQL_FLOAT>( 1, _ncols );
};

// ----------------------------------------------------------------------------

void LinearRegression::fit(
    const containers::Matrix<AUTOSQL_FLOAT>& _new_feature,
    const containers::Matrix<AUTOSQL_FLOAT>& _residuals,
    const containers::Matrix<AUTOSQL_FLOAT>& _sample_weights )
{
    // ----------------------------------------------------

    assert( _new_feature.nrows() == _residuals.nrows() && "UpdateRates::fit" );
    assert(
        _new_feature.nrows() == _sample_weights.nrows() && "UpdateRates::fit" );

    assert( _new_feature.ncols() == 1 && "UpdateRates::fit" );
    assert( _residuals.ncols() > 0 && "UpdateRates::fit" );

    assert( _sample_weights.ncols() == 1 && "UpdateRates::fit" );

    // ----------------------------------------------------

    containers::Matrix<AUTOSQL_FLOAT> means( 1, _residuals.ncols() + 1 );

    AUTOSQL_FLOAT& mean_new_feature = *means.data();

    AUTOSQL_FLOAT* const mean_residuals = means.data() + 1;

    // ----------------------------------------------------
    // Calculate mean_yhat and mean_residuals
    // (both stored in means)

    for ( AUTOSQL_INT i = 0; i < _new_feature.nrows(); ++i )
        {
            mean_new_feature += _new_feature[i] * _sample_weights[i];
        }

    for ( AUTOSQL_INT i = 0; i < _residuals.nrows(); ++i )
        {
            for ( AUTOSQL_INT j = 0; j < _residuals.ncols(); ++j )
                {
                    mean_residuals[j] +=
                        _residuals( i, j ) * _sample_weights[i];
                }
        }

#ifdef AUTOSQL_PARALLEL

    containers::Matrix<AUTOSQL_FLOAT> global_means( 1, means.ncols() );

    AUTOSQL_PARALLEL_LIB::all_reduce(
        comm(),
        means.data(),
        means.ncols(),
        global_means.data(),
        std::plus<AUTOSQL_FLOAT>() );

    comm().barrier();

    std::copy( global_means.begin(), global_means.end(), means.begin() );

#endif  // AUTOSQL_PARALLEL

    {
        AUTOSQL_FLOAT sum_sample_weights = std::accumulate(
            _sample_weights.begin(), _sample_weights.end(), 0.0 );

#ifdef AUTOSQL_PARALLEL

        AUTOSQL_FLOAT global_sum_sample_weights;

        AUTOSQL_PARALLEL_LIB::all_reduce(
            comm(),
            sum_sample_weights,
            global_sum_sample_weights,
            std::plus<AUTOSQL_FLOAT>() );

        comm().barrier();

        sum_sample_weights = global_sum_sample_weights;

#endif  // AUTOSQL_PARALLEL

        std::for_each(
            means.begin(),
            means.end(),
            [sum_sample_weights]( AUTOSQL_FLOAT& m ) {
                m /= sum_sample_weights;
            } );
    }

    // ----------------------------------------------------
    // Calculate var_yhat and cov_yhat
    // (both stored in covars)

    containers::Matrix<AUTOSQL_FLOAT> covars( 1, _residuals.ncols() + 1 );

    AUTOSQL_FLOAT& var_new_feature = *covars.data();

    AUTOSQL_FLOAT* const cov_new_feature = covars.data() + 1;

    // -----

    for ( AUTOSQL_INT i = 0; i < _new_feature.nrows(); ++i )
        {
            var_new_feature += ( _new_feature[i] - mean_new_feature ) *
                               ( _new_feature[i] - mean_new_feature ) *
                               _sample_weights[i];
        }

    // -----

    for ( AUTOSQL_INT i = 0; i < _residuals.nrows(); ++i )
        {
            for ( AUTOSQL_INT j = 0; j < _residuals.ncols(); ++j )
                {
                    cov_new_feature[j] +=
                        ( _residuals( i, j ) - mean_residuals[j] ) *
                        ( _new_feature[i] - mean_new_feature ) *
                        _sample_weights[i];
                }
        }

            // -----

#ifdef AUTOSQL_PARALLEL

    containers::Matrix<AUTOSQL_FLOAT> global_covars( 1, covars.ncols() );

    AUTOSQL_PARALLEL_LIB::all_reduce(
        comm(),
        covars.data(),
        covars.ncols(),
        global_covars.data(),
        std::plus<AUTOSQL_FLOAT>() );

    comm().barrier();

    std::copy( global_covars.begin(), global_covars.end(), covars.begin() );

#endif  // AUTOSQL_PARALLEL

    // ------------------
    // Calculate slopes_

    slopes_ = containers::Matrix<AUTOSQL_FLOAT>( 1, _residuals.ncols() );

    for ( AUTOSQL_INT j = 0; j < _residuals.ncols(); ++j )
        {
            slopes_[j] = cov_new_feature[j] / var_new_feature;
        }

    // ---------------------
    // Calculate intercepts_

    intercepts_ = containers::Matrix<AUTOSQL_FLOAT>( 1, _residuals.ncols() );

    for ( AUTOSQL_INT j = 0; j < _residuals.ncols(); ++j )
        {
            intercepts_[j] = mean_residuals[j] - slopes_[j] * mean_new_feature;
        }

    // ---------------------
    // Make sure that slopes_ and intercepts_ are in range

    auto in_range = []( AUTOSQL_FLOAT& val ) {
        val = ( ( std::isnan( val ) || std::isinf( val ) ) ? 0.0 : val );
    };

    std::for_each( slopes_.begin(), slopes_.end(), in_range );

    std::for_each( intercepts_.begin(), intercepts_.end(), in_range );

    // ---------------------
}

// ----------------------------------------------------------------------------

containers::Matrix<AUTOSQL_FLOAT> LinearRegression::predict(
    const containers::Matrix<AUTOSQL_FLOAT>& _yhat ) const
{
    containers::Matrix<AUTOSQL_FLOAT> f_t( _yhat.nrows(), intercepts_.ncols() );

    for ( AUTOSQL_INT i = 0; i < f_t.nrows(); ++i )
        {
            for ( AUTOSQL_INT j = 0; j < f_t.ncols(); ++j )
                {
                    f_t( i, j ) = slopes_[j] * _yhat[i] + intercepts_[j];
                }
        }

    return f_t;
}

// ----------------------------------------------------------------------------
}
