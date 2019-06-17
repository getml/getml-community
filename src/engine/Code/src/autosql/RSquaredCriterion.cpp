#include "autosql/optimizationcriteria/optimizationcriteria.hpp"

namespace autosql
{
namespace optimizationcriteria
{
// ----------------------------------------------------------------------------

RSquaredCriterion::RSquaredCriterion(
    const std::shared_ptr<const descriptors::Hyperparameters>& _hyperparameters,
    const size_t _num_rows )
    : OptimizationCriterion(),
      hyperparameters_( _hyperparameters ),
      impl_( OptimizationCriterionImpl( _hyperparameters, _num_rows ) ){};

// ----------------------------------------------------------------------------

std::vector<AUTOSQL_INT> RSquaredCriterion::argsort(
    const AUTOSQL_INT _begin, const AUTOSQL_INT _end ) const
{
    // ---------------------------------------------------------------------

    debug_log( "argsort..." );

    assert( _begin <= _end );

    assert( _begin >= 0 );
    assert( _end <= impl().storage_ix() );

    debug_log( "Preparing sufficient statistics..." );

    const auto sufficient_statistics =
        impl().reduce_sufficient_statistics_stored();

    // ---------------------------------------------------------------------

    debug_log( "Calculating values..." );

    std::vector<AUTOSQL_FLOAT> values( _end - _begin );

    for ( AUTOSQL_INT i = _begin; i < _end; ++i )
        {
            values[i - _begin] =
                calculate_r_squared( i, sufficient_statistics );
        }

    // ---------------------------------------------------------------------

    debug_log( "Calculating indices..." );

    std::vector<AUTOSQL_INT> indices( _end - _begin );

    for ( AUTOSQL_INT i = 0; i < _end - _begin; ++i )
        {
            indices[i] = i;
        }

    std::sort(
        indices.begin(),
        indices.end(),
        [&values]( const AUTOSQL_INT ix1, const AUTOSQL_INT ix2 ) {
            return values[ix1] > values[ix2];
        } );

    // ---------------------------------------------------------------------

    return indices;

    // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

AUTOSQL_FLOAT RSquaredCriterion::calculate_r_squared(
    const size_t _i,
    const std::deque<std::vector<AUTOSQL_FLOAT>>& _sufficient_statistics ) const
{
    assert( sample_weights_.size() == y_centered_[0].size() );

    assert( _i < _sufficient_statistics.size() );

    assert( _sufficient_statistics[_i].size() == y_.size() + 4 );

    assert( sum_y_centered_y_centered_.size() == y_.size() );

    const AUTOSQL_FLOAT sum_yhat = _sufficient_statistics[_i][0];

    assert( sum_yhat == sum_yhat );

    const AUTOSQL_FLOAT sum_yhat_yhat = _sufficient_statistics[_i][1];

    assert( sum_yhat_yhat == sum_yhat_yhat );

    AUTOSQL_FLOAT r_squared = 0.0;

    for ( size_t j = 0; j < y_.size(); ++j )
        {
            const AUTOSQL_FLOAT sum_y_centered_yhat =
                _sufficient_statistics[_i][2 + j];

            assert( !std::isnan( sum_y_centered_yhat ) );

            const AUTOSQL_FLOAT var_yhat =
                sum_sample_weights_ * sum_yhat_yhat - sum_yhat * sum_yhat;

            if ( var_yhat == 0.0 || sum_y_centered_y_centered_[j] == 0.0 )
                {
                    continue;
                }

            r_squared +=
                sum_sample_weights_ * ( sum_y_centered_yhat / var_yhat ) *
                ( sum_y_centered_yhat / sum_y_centered_y_centered_[j] );
        }

    assert( sample_weights_.size() == y_centered_[0].size() );

    return r_squared;
}

// ----------------------------------------------------------------------------

AUTOSQL_INT RSquaredCriterion::find_maximum()
{
    assert( sample_weights_.size() == y_centered_[0].size() );

    AUTOSQL_INT max_ix = 0;

    debug_log( "Preparing sufficient statistics..." );

    const auto sufficient_statistics =
        impl().reduce_sufficient_statistics_stored();

    debug_log( "Finding maximum..." );

    assert( sample_weights_.size() == y_centered_[0].size() );

    assert( impl().storage_ix() == sufficient_statistics.size() );

    impl().values_stored().resize( impl().storage_ix() );

    for ( size_t i = 0; i < sufficient_statistics.size(); ++i )
        {
            assert( i < impl().values_stored().size() );

            impl().values_stored()[i] = 0.0;

            // num_samples_smaller and num_samples_greater are always the
            // elements in the last two columns of
            // sufficient_statistics_stored_, which is why
            // sufficient_statistics_stored_ has two extra columns over
            // ..._current and ..._committed.

            assert( sufficient_statistics[i].size() >= 2 );

            const AUTOSQL_FLOAT num_samples_smaller =
                sufficient_statistics[i][sufficient_statistics[i].size() - 2];

            const AUTOSQL_FLOAT num_samples_greater =
                sufficient_statistics[i][sufficient_statistics[i].size() - 1];

            assert( hyperparameters_ );

            assert( hyperparameters_->tree_hyperparameters_ );

            const auto min_num_samples = static_cast<AUTOSQL_FLOAT>(
                hyperparameters_->tree_hyperparameters_->min_num_samples_ );

            // If the split would result in an insufficient number
            // of samples on any node, it will not be considered.
            if ( num_samples_smaller < min_num_samples ||
                 num_samples_greater < min_num_samples )
                {
                    continue;
                }

            impl().values_stored()[i] =
                calculate_r_squared( i, sufficient_statistics );

            if ( impl().values_stored()[i] > impl().values_stored()[max_ix] )
                {
                    max_ix = static_cast<AUTOSQL_INT>( i );
                }
        }

    impl().set_max_ix( max_ix );

    debug_log( "Done finding maximum..." );

    assert( sample_weights_.size() == y_centered_[0].size() );

    return max_ix;
}

// ----------------------------------------------------------------------------

void RSquaredCriterion::init(
    const std::vector<std::vector<AUTOSQL_FLOAT>>& _y,
    const std::vector<AUTOSQL_FLOAT>& _sample_weights )
{
    // ---------------------------------------------------------------------

    assert( _sample_weights.size() > 0 );

    debug_log( "Optimization init " );

    debug_log(
        "_sample_weights.size(): " + std::to_string( _sample_weights.size() ) );

    // ---------------------------------------------------------------------

    y_ = _y;

    sample_weights_ = _sample_weights;

    sufficient_statistics_committed_ =
        std::vector<AUTOSQL_FLOAT>( y_.size() + 2 );

    sufficient_statistics_current_ =
        std::vector<AUTOSQL_FLOAT>( y_.size() + 2 );

    sum_yhat_committed_ = sufficient_statistics_committed_.data();
    sum_yhat_current_ = sufficient_statistics_current_.data();

    sum_yhat_yhat_committed_ = sufficient_statistics_committed_.data() + 1;
    sum_yhat_yhat_current_ = sufficient_statistics_current_.data() + 1;

    sum_y_centered_yhat_committed_ =
        sufficient_statistics_committed_.data() + 2;

    sum_y_centered_yhat_current_ = sufficient_statistics_current_.data() + 2;

    sum_y_centered_y_centered_ = std::vector<AUTOSQL_FLOAT>( y_.size() );

    y_centered_ = std::vector<std::vector<AUTOSQL_FLOAT>>(
        y_.size(), std::vector<AUTOSQL_FLOAT>( _sample_weights.size() ) );

    // ---------------------------------------------------------------------
    // Calculate sum_sample_weights_

    sum_sample_weights_ =
        std::accumulate( sample_weights_.begin(), sample_weights_.end(), 0.0 );

    utils::Reducer::reduce(
        std::plus<AUTOSQL_FLOAT>(), &sum_sample_weights_, comm_ );

    // ---------------------------------------------------------------------
    // Calculate y_mean

    auto y_mean = std::vector<AUTOSQL_FLOAT>( y_.size() );

    for ( size_t j = 0; j < y_.size(); ++j )
        {
            assert( sample_weights_.size() == y_[j].size() );

            for ( size_t i = 0; i < y_[j].size(); ++i )
                {
                    assert(
                        !std::isnan( y_[j][i] ) && !std::isinf( y_[j][i] ) );
                    assert(
                        !std::isnan( sample_weights_[i] ) &&
                        !std::isinf( sample_weights_[i] ) );

                    y_mean[j] += y_[j][i] * sample_weights_[i];
                }
        }

    utils::Reducer::reduce( std::plus<AUTOSQL_FLOAT>(), &y_mean, comm_ );

    for ( auto& ym : y_mean )
        {
            ym /= sum_sample_weights_;
        }

    // ---------------------------------------------------------------------
    // Calculate y_centered

    for ( size_t j = 0; j < y_.size(); ++j )
        {
            assert( y_centered_[j].size() == y_[j].size() );

            for ( size_t i = 0; i < y_[j].size(); ++i )
                {
                    assert( !std::isnan( y_[j][i] ) );
                    assert( !std::isnan( y_mean[j] ) );

                    y_centered_[j][i] = y_[j][i] - y_mean[j];
                }
        }

    // ---------------------------------------------------------------------
    // Calculate sum_y_centered_y_centered_

    assert( sum_y_centered_y_centered_.size() == y_.size() );

    for ( size_t j = 0; j < y_.size(); ++j )
        {
            assert( y_centered_[j].size() == y_[j].size() );
            assert( y_centered_[j].size() == sample_weights_.size() );

            for ( size_t i = 0; i < y_[j].size(); ++i )
                {
                    assert( !std::isnan( y_centered_[j][i] ) );
                    assert( !std::isnan( sample_weights_[i] ) );

                    sum_y_centered_y_centered_[j] += y_centered_[j][i] *
                                                     y_centered_[j][i] *
                                                     sample_weights_[i];
                }

            assert( !std::isnan( sum_y_centered_y_centered_[j] ) );
        }

    utils::Reducer::reduce(
        std::plus<AUTOSQL_FLOAT>(), &sum_y_centered_y_centered_, comm_ );

    for ( size_t j = 0; j < sum_y_centered_y_centered_.size(); ++j )
        {
            assert( !std::isnan( sum_y_centered_y_centered_[j] ) );
        }

    debug_log(
        "y_centered_[0].size(): " + std::to_string( y_centered_[0].size() ) );

    // ---------------------------------------------------------------------

    debug_log( "Optimization done." );

    assert( sample_weights_.size() == y_centered_[0].size() );

    // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void RSquaredCriterion::init_yhat(
    const std::vector<AUTOSQL_FLOAT>& _yhat,
    const containers::IntSet& _indices )
{
    // ---------------------------------------------------------------------

    assert( sample_weights_.size() == y_centered_[0].size() );

    // ---------------------------------------------------------------------

    debug_log( "init_yhat..." );

    debug_log(
        "y_centered_[0].size(): " + std::to_string( y_centered_[0].size() ) );

    // ---------------------------------------------------------------------
    // Calculate y_hat_mean_

    y_hat_mean_ = 0.0;

    for ( size_t i = 0; i < _yhat.size(); ++i )
        {
            y_hat_mean_ += _yhat[i] * sample_weights_[i];
        }

    utils::Reducer::reduce( std::plus<AUTOSQL_FLOAT>(), &y_hat_mean_, comm_ );

    y_hat_mean_ /= sum_sample_weights_;

    // ---------------------------------------------------------------------
    // sum_yhat_current_ is 0.0 by definition after calling init(...)
    // (because it is defined as the sum of all y_hat - y_hat_mean_)

    *sum_yhat_current_ = 0.0;

    // ---------------------------------------------------------------------
    // Calculate sum_yhat_yhat_current_

    *sum_yhat_yhat_current_ = 0.0;

    assert( _yhat.size() == sample_weights_.size() );

    for ( size_t i = 0; i < _yhat.size(); ++i )
        {
            *sum_yhat_yhat_current_ += ( _yhat[i] - y_hat_mean_ ) *
                                       ( _yhat[i] - y_hat_mean_ ) *
                                       sample_weights_[i];
        }

    // ---------------------------------------------------------------------
    // Calculate sum_y_centered_yhat_current_

    assert( y_.size() == y_centered_.size() );

    std::fill(
        sum_y_centered_yhat_current_,
        sum_y_centered_yhat_current_ + y_.size(),
        0.0 );

    for ( size_t j = 0; j < y_.size(); ++j )
        {
            assert( _yhat.size() == y_centered_[j].size() );

            for ( size_t i = 0; i < _yhat.size(); ++i )
                {
                    sum_y_centered_yhat_current_[j] +=
                        ( _yhat[i] - y_hat_mean_ ) * y_centered_[j][i] *
                        sample_weights_[i];
                }
        }

    // ---------------------------------------------------------------------

    debug_log( "init_yhat...done" );

    // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void RSquaredCriterion::update_samples(
    const containers::IntSet& _indices,
    const std::vector<AUTOSQL_FLOAT>& _new_values,
    const std::vector<AUTOSQL_FLOAT>& _old_values )
{
    for ( auto ix : _indices )
        {
            const AUTOSQL_FLOAT new_value = _new_values[ix] - y_hat_mean_;
            const AUTOSQL_FLOAT old_value = _old_values[ix] - y_hat_mean_;

            sum_yhat_current_[0] +=
                ( new_value - old_value ) * sample_weights_[ix];

            sum_yhat_yhat_current_[0] +=
                ( new_value * new_value - old_value * old_value ) *
                sample_weights_[ix];

            for ( size_t j = 0; j < y_.size(); ++j )
                {
                    assert( ix < y_centered_[j].size() );

                    sum_y_centered_yhat_current_[j] +=
                        y_centered_[j][ix] * ( new_value - old_value ) *
                        sample_weights_[ix];
                }
        }

    assert( sample_weights_.size() == y_centered_[0].size() );
}

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace autosql
