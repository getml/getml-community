#include "optimizationcriteria/optimizationcriteria.hpp"

namespace autosql
{
namespace optimizationcriteria
{
// ----------------------------------------------------------------------------

RSquaredCriterion::RSquaredCriterion( const SQLNET_FLOAT _min_num_samples )
    : OptimizationCriterion(), min_num_samples_( _min_num_samples ){};

// ----------------------------------------------------------------------------

std::vector<SQLNET_INT> RSquaredCriterion::argsort(
    const SQLNET_INT _begin, const SQLNET_INT _end ) const
{
    // ---------------------------------------------------------------------

    debug_message( "argsort..." );

    assert( _begin <= _end );

    assert( _begin >= 0 );
    assert( _end <= impl().storage_ix() );

    debug_message( "Preparing sufficient statistics..." );

    const auto sufficient_statistics =
        impl().reduce_sufficient_statistics_stored();

    // ---------------------------------------------------------------------

    debug_message( "Calculating values..." );

    std::vector<SQLNET_FLOAT> values( _end - _begin );

    for ( SQLNET_INT i = _begin; i < _end; ++i )
        {
            values[i - _begin] =
                calculate_r_squared( i, sufficient_statistics );
        }

    // ---------------------------------------------------------------------

    debug_message( "Calculating indices..." );

    std::vector<SQLNET_INT> indices( _end - _begin );

    for ( SQLNET_INT i = 0; i < _end - _begin; ++i )
        {
            indices[i] = i;
        }

    std::sort(
        indices.begin(),
        indices.end(),
        [&values]( const SQLNET_INT ix1, const SQLNET_INT ix2 ) {
            return values[ix1] > values[ix2];
        } );

    // ---------------------------------------------------------------------

    return indices;

    // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

SQLNET_FLOAT RSquaredCriterion::calculate_r_squared(
    const SQLNET_INT _i,
    const containers::Matrix<SQLNET_FLOAT>& _sufficient_statistics ) const
{
    assert( _i < impl().storage_ix() );

    const SQLNET_FLOAT sum_yhat = _sufficient_statistics( _i, 0 );

    assert( sum_yhat == sum_yhat );

    const SQLNET_FLOAT sum_yhat_yhat = _sufficient_statistics( _i, 1 );

    assert( sum_yhat_yhat == sum_yhat_yhat );

    SQLNET_FLOAT r_squared = 0.0;

    for ( SQLNET_INT j = 0; j < y_.ncols(); ++j )
        {
            const SQLNET_FLOAT sum_y_centered_yhat =
                _sufficient_statistics( _i, 2 + j );

            assert( sum_y_centered_yhat == sum_y_centered_yhat );

            const SQLNET_FLOAT var_yhat =
                sum_sample_weights_ * sum_yhat_yhat - sum_yhat * sum_yhat;

            if ( var_yhat == 0.0 || sum_y_centered_y_centered_( 0, j ) == 0.0 )
                {
                    continue;
                }

            r_squared +=
                sum_sample_weights_ * ( sum_y_centered_yhat / var_yhat ) *
                ( sum_y_centered_yhat / sum_y_centered_y_centered_( 0, j ) );
        }

    return r_squared;
}

// ----------------------------------------------------------------------------

SQLNET_INT RSquaredCriterion::find_maximum()
{
    SQLNET_INT max_ix = 0;

    debug_message( "Preparing sufficient statistics..." );

    const auto sufficient_statistics =
        impl().reduce_sufficient_statistics_stored();

    debug_message( "Finding maximum..." );

    for ( SQLNET_INT i = 0; i < impl().storage_ix(); ++i )
        {
            impl().values_stored()[i] = 0.0;

            // num_samples_smaller and num_samples_greater are always the
            // elements in the last two columns of
            // sufficient_statistics_stored_, which is why
            // sufficient_statistics_stored_ has two extra columns over
            // ..._current and ..._committed.

            const SQLNET_FLOAT num_samples_smaller =
                sufficient_statistics( i, sufficient_statistics.ncols() - 2 );

            const SQLNET_FLOAT num_samples_greater =
                sufficient_statistics( i, sufficient_statistics.ncols() - 1 );

            // If the split would result in an insufficient number
            // of samples on any node, it will not be considered.
            if ( num_samples_smaller < min_num_samples_ ||
                 num_samples_greater < min_num_samples_ )
                {
                    continue;
                }

            impl().values_stored()[i] =
                calculate_r_squared( i, sufficient_statistics );

            if ( impl().values_stored()[i] > impl().values_stored()[max_ix] )
                {
                    max_ix = i;
                }
        }

    impl().set_max_ix( max_ix );

    return max_ix;
}

// ----------------------------------------------------------------------------

void RSquaredCriterion::init(
    containers::Matrix<SQLNET_FLOAT>& _y,
    containers::Matrix<SQLNET_FLOAT>& _sample_weights )
{
    y_ = _y;

    sample_weights_ = _sample_weights;

    sufficient_statistics_committed_ =
        containers::Matrix<SQLNET_FLOAT>( 1, y_.ncols() + 2 );
    sufficient_statistics_current_ =
        containers::Matrix<SQLNET_FLOAT>( 1, y_.ncols() + 2 );

    sum_yhat_committed_ = sufficient_statistics_committed_.data();
    sum_yhat_current_ = sufficient_statistics_current_.data();

    sum_yhat_yhat_committed_ = sufficient_statistics_committed_.data() + 1;
    sum_yhat_yhat_current_ = sufficient_statistics_current_.data() + 1;

    sum_y_centered_yhat_committed_ =
        sufficient_statistics_committed_.data() + 2;
    sum_y_centered_yhat_current_ = sufficient_statistics_current_.data() + 2;

    sum_y_centered_y_centered_ =
        containers::Matrix<SQLNET_FLOAT>( 1, y_.ncols() );

    y_centered_ = containers::Matrix<SQLNET_FLOAT>( y_.nrows(), y_.ncols() );

    // ---------------------------------------------------------------------
    // Calculate sum_sample_weights_

    sum_sample_weights_ =
        std::accumulate( sample_weights_.begin(), sample_weights_.end(), 0.0 );

// Calculate global sum over all processes
#ifdef SQLNET_PARALLEL

    SQLNET_FLOAT sum_sample_weights_global;

    SQLNET_PARALLEL_LIB::all_reduce(
        *comm_,                     // comm
        sum_sample_weights_,        // in_value
        sum_sample_weights_global,  // out_value
        std::plus<SQLNET_FLOAT>()   // op
    );

    comm_->barrier();

    sum_sample_weights_ = sum_sample_weights_global;

#endif  // SQLNET_PARALLEL

    // ---------------------------------------------------------------------
    // Calculate y_mean

    containers::Matrix<SQLNET_FLOAT> y_mean( 1, y_.ncols() );

    for ( SQLNET_INT i = 0; i < y_.nrows(); ++i )
        {
            for ( SQLNET_INT j = 0; j < y_.ncols(); ++j )
                {
                    assert( y_( i, j ) == y_( i, j ) );
                    assert( sample_weights_[i] == sample_weights_[i] );
                    y_mean( 0, j ) += y_( i, j ) * sample_weights_[i];
                }
        }

// Calculate global sum over all processes
#ifdef SQLNET_PARALLEL

    containers::Matrix<SQLNET_FLOAT> y_mean_global( 1, y_.ncols() );

    SQLNET_PARALLEL_LIB::all_reduce(
        *comm_,                    // comm
        y_mean.data(),             // in_values
        y_.ncols(),                // count,
        y_mean_global.data(),      // out_values
        std::plus<SQLNET_FLOAT>()  // op
    );

    comm_->barrier();

    y_mean = y_mean_global;

#endif  // SQLNET_PARALLEL

    for ( auto& ym : y_mean )
        {
            ym /= sum_sample_weights_;
        }

    // ---------------------------------------------------------------------
    // Calculate y_centered

    for ( SQLNET_INT i = 0; i < y_.nrows(); ++i )
        {
            for ( SQLNET_INT j = 0; j < y_.ncols(); ++j )
                {
                    y_centered_( i, j ) = y_( i, j ) - y_mean( 0, j );
                }
        }

    // ---------------------------------------------------------------------
    // Calculate sum_y_centered_y_centered_

    for ( SQLNET_INT i = 0; i < y_.nrows(); ++i )
        {
            for ( SQLNET_INT j = 0; j < y_.ncols(); ++j )
                {
                    sum_y_centered_y_centered_( 0, j ) += y_centered_( i, j ) *
                                                          y_centered_( i, j ) *
                                                          sample_weights_[i];

                    assert(
                        sum_y_centered_y_centered_( 0, j ) ==
                        sum_y_centered_y_centered_( 0, j ) );
                }
        }

// Calculate global sum over all processes
#ifdef SQLNET_PARALLEL

    containers::Matrix<SQLNET_FLOAT> sum_y_centered_y_centered_global(
        1, sum_y_centered_y_centered_.ncols() );

    SQLNET_PARALLEL_LIB::all_reduce(
        *comm_,                                   // comm
        sum_y_centered_y_centered_.data(),        // in_values
        y_.ncols(),                               // count,
        sum_y_centered_y_centered_global.data(),  // out_values
        std::plus<SQLNET_FLOAT>()                 // op
    );

    comm_->barrier();

    sum_y_centered_y_centered_ = sum_y_centered_y_centered_global;

#endif  // SQLNET_PARALLEL

    for ( SQLNET_INT j = 0; j < sum_y_centered_y_centered_.ncols(); ++j )
        {
            assert(
                sum_y_centered_y_centered_( 0, j ) ==
                sum_y_centered_y_centered_( 0, j ) );
        }
}

// ----------------------------------------------------------------------------

void RSquaredCriterion::init_yhat(
    const containers::Matrix<SQLNET_FLOAT>& _yhat,
    const containers::IntSet& _indices )
{
    // ---------------------------------------------------------------------

    debug_message( "init_yhat..." );

    // ---------------------------------------------------------------------
    // Calculate y_hat_mean_

    y_hat_mean_ = 0.0;

    for ( SQLNET_INT i = 0; i < _yhat.nrows(); ++i )
        {
            y_hat_mean_ += _yhat[i] * sample_weights_[i];
        }

// Calculate global sum over all processes
#ifdef SQLNET_PARALLEL

    SQLNET_FLOAT y_hat_mean_global;

    SQLNET_PARALLEL_LIB::all_reduce(
        *comm_,                    // comm
        y_hat_mean_,               // in_value
        y_hat_mean_global,         // out_value
        std::plus<SQLNET_FLOAT>()  // op
    );

    comm_->barrier();

    y_hat_mean_ = y_hat_mean_global;

#endif  // SQLNET_PARALLEL

    y_hat_mean_ /= sum_sample_weights_;

    // ---------------------------------------------------------------------
    // sum_yhat_current_ is 0.0 by definition after calling init(...)
    // (because it is defined as the sum of all y_hat - y_hat_mean_)

    *sum_yhat_current_ = 0.0;

    // ---------------------------------------------------------------------
    // Calculate sum_yhat_yhat_current_ and
    // sum_y_centered_yhat_current_

    *sum_yhat_yhat_current_ = 0.0;

    for ( SQLNET_INT j = 0; j < y_.ncols(); ++j )
        {
            sum_y_centered_yhat_current_[j] = 0.0;
        }

    for ( SQLNET_INT i = 0; i < _yhat.nrows(); ++i )
        {
            *sum_yhat_yhat_current_ += ( _yhat[i] - y_hat_mean_ ) *
                                       ( _yhat[i] - y_hat_mean_ ) *
                                       sample_weights_[i];

            for ( SQLNET_INT j = 0; j < y_.ncols(); ++j )
                {
                    sum_y_centered_yhat_current_[j] +=
                        ( _yhat[i] - y_hat_mean_ ) * y_centered_( i, j ) *
                        sample_weights_[i];
                }
        }

    debug_message( "init_yhat...done" );
}

// ----------------------------------------------------------------------------

void RSquaredCriterion::update_samples(
    const containers::IntSet& _indices,
    const containers::Matrix<SQLNET_FLOAT>& _new_values,
    const std::vector<SQLNET_FLOAT>& _old_values )
{
    for ( auto ix : _indices )
        {
            const SQLNET_FLOAT new_value = _new_values[ix] - y_hat_mean_;
            const SQLNET_FLOAT old_value = _old_values[ix] - y_hat_mean_;

            sum_yhat_current_[0] +=
                ( new_value - old_value ) * sample_weights_[ix];

            sum_yhat_yhat_current_[0] +=
                ( new_value * new_value - old_value * old_value ) *
                sample_weights_[ix];

            for ( SQLNET_INT j = 0; j < y_.ncols(); ++j )
                {
                    sum_y_centered_yhat_current_[j] +=
                        y_centered_( ix, j ) * ( new_value - old_value ) *
                        sample_weights_[ix];
                }
        }
}

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace autosql
