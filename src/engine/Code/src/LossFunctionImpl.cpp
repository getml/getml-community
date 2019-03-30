#include "lossfunctions/lossfunctions.hpp"

namespace relboost
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

RELBOOST_FLOAT LossFunctionImpl::calc_regularization_reduction(
    const std::vector<RELBOOST_FLOAT>& _eta1,
    const std::vector<RELBOOST_FLOAT>& _eta2,
    const std::vector<size_t>& _indices,
    const RELBOOST_FLOAT _old_intercept,
    const RELBOOST_FLOAT _old_weight,
    const std::array<RELBOOST_FLOAT, 3>& _weights ) const
{
    assert( !std::isnan( std::get<0>( _weights ) ) );
    assert( !std::isnan( _old_intercept ) );

    assert( _eta1.size() == _eta2.size() );
    assert( _eta1.size() == targets().size() );

    assert( sample_weights_ );
    assert( _eta1.size() == sample_weights_->size() );

    if ( hyperparameters().lambda_ == 0.0 )
        {
            return 0.0;
        }

    const auto num_targets = static_cast<RELBOOST_FLOAT>( targets().size() );

    RELBOOST_FLOAT regularization =
        num_targets * ( _old_intercept * _old_intercept -
                        std::get<0>( _weights ) * std::get<0>( _weights ) );

    if ( std::isnan( std::get<1>( _weights ) ) )
        {
            // In the case of of the weights being nan, _eta2 plays the role of
            // _eta_old and _eta1 plays _eta_new.

            regularization += calc_regularization_reduction(
                _eta2, _eta1, _indices, _old_weight, std::get<2>( _weights ) );
        }
    else if ( std::isnan( std::get<2>( _weights ) ) )
        {
            // In the case of of the weights being nan, _eta2 plays the role of
            // _eta_old and _eta1 plays _eta_new.

            regularization += calc_regularization_reduction(
                _eta2, _eta1, _indices, _old_weight, std::get<1>( _weights ) );
        }
    else
        {
            for ( size_t ix : _indices )
                {
                    assert( ix < targets().size() );
                    regularization +=
                        sample_weights( ix ) *
                        ( _old_weight * _old_weight *
                              ( _eta1[ix] + _eta2[ix] ) -
                          std::get<1>( _weights ) * std::get<1>( _weights ) *
                              _eta1[ix] +
                          std::get<2>( _weights ) * std::get<2>( _weights ) *
                              _eta2[ix] );
                }
        }

    return 0.5 * hyperparameters().lambda_ * regularization;
}

// ----------------------------------------------------------------------------

RELBOOST_FLOAT LossFunctionImpl::calc_regularization_reduction(
    const std::vector<RELBOOST_FLOAT>& _eta_old,
    const std::vector<RELBOOST_FLOAT>& _eta_new,
    const std::vector<size_t>& _indices,
    const RELBOOST_FLOAT _old_weight,
    const RELBOOST_FLOAT _new_weight ) const
{
    assert( _eta_old.size() == targets().size() );
    assert( _eta_new.size() == targets().size() );

    assert( sample_weights_ );
    assert( sample_weights_->size() == targets().size() );

    RELBOOST_FLOAT regularization = 0.0;

    if ( std::isnan( _old_weight ) )
        {
            for ( size_t ix : _indices )
                {
                    assert( ix < targets().size() );
                    regularization -=
                        sample_weights( ix ) *
                        ( _new_weight * _new_weight * _eta_new[ix] );
                }
        }
    else
        {
            for ( size_t ix : _indices )
                {
                    assert( ix < targets().size() );
                    regularization +=
                        sample_weights( ix ) *
                        ( _old_weight * _old_weight * _eta_old[ix] -
                          _new_weight * _new_weight * _eta_new[ix] );
                }
        }

    return regularization;
}

// ----------------------------------------------------------------------------

void LossFunctionImpl::calc_sums(
    const std::vector<RELBOOST_FLOAT>& _sample_weights,
    RELBOOST_FLOAT* _sum_g,
    RELBOOST_FLOAT* _sum_h ) const
{
    assert( g_.size() == _sample_weights.size() );
    assert( h_.size() == _sample_weights.size() );

    *_sum_g = std::inner_product(
        g_.begin(), g_.end(), _sample_weights.begin(), 0.0 );

    *_sum_h = std::inner_product(
        h_.begin(), h_.end(), _sample_weights.begin(), 0.0 );
}

// ----------------------------------------------------------------------------

RELBOOST_FLOAT LossFunctionImpl::calc_update_rate(
    const std::vector<RELBOOST_FLOAT>& _yhat_old,
    const std::vector<RELBOOST_FLOAT>& _predictions ) const
{
    // ------------------------------------------------------------------------

    assert( _yhat_old.size() == _predictions.size() );
    assert( _yhat_old.size() == targets().size() );

    assert( _yhat_old.size() == g_.size() );
    assert( _yhat_old.size() == h_.size() );

    // ------------------------------------------------------------------------

    RELBOOST_FLOAT sum_g_predictions = 0.0;

    for ( size_t i = 0; i < _predictions.size(); ++i )
        {
            sum_g_predictions += g_[i] * _predictions[i];
        }

    // ------------------------------------------------------------------------

    RELBOOST_FLOAT sum_h_predictions = 0.0;

    for ( size_t i = 0; i < _predictions.size(); ++i )
        {
            sum_h_predictions += h_[i] * _predictions[i] * _predictions[i];
        }

    // ------------------------------------------------------------------------

    if ( sum_h_predictions == 0.0 )
        {
            return 0.0;
        }
    else
        {
            return -sum_g_predictions / sum_h_predictions;
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<std::array<RELBOOST_FLOAT, 3>> LossFunctionImpl::calc_weights(
    const enums::Update _update,
    const RELBOOST_FLOAT _old_weight,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _split_begin,
    const std::vector<const containers::Match*>::iterator _split_end,
    const std::vector<const containers::Match*>::iterator _end ) const
{
    // ------------------------------------------------------------------------
    // Note the minus!

    const auto calc_g =
        [this]( const RELBOOST_FLOAT init, const containers::Match* ptr ) {
            return init - g_[ptr->ix_output];
        };

    auto g2 = std::accumulate( _begin, _split_begin, 0.0, calc_g );

    const auto g1 = std::accumulate( _split_begin, _split_end, 0.0, calc_g );

    g2 += std::accumulate( _split_end, _end, 0.0, calc_g );

    // ------------------------------------------------------------------------

    const auto calc_h = [this](
                            const RELBOOST_FLOAT init,
                            const containers::Match* ptr ) {
        return init + h_[ptr->ix_output] * ( 1.0 + hyperparameters().lambda_ );
    };

    auto h2 = std::accumulate( _begin, _split_begin, 0.0, calc_h );

    const auto h1 = std::accumulate( _split_begin, _split_end, 0.0, calc_h );

    h2 += std::accumulate( _split_end, _end, 0.0, calc_h );

    // ------------------------------------------------------------------------
    // In this case, it is impossible for the weights to be NAN.

    const auto arr = std::array<RELBOOST_FLOAT, 3>( {0.0, g1 / h1, g2 / h2} );

    return std::vector<std::array<RELBOOST_FLOAT, 3>>( {arr} );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::array<RELBOOST_FLOAT, 3> LossFunctionImpl::calc_weights_avg_null(
    const enums::Aggregation _agg,
    const RELBOOST_FLOAT _old_weight,
    const std::vector<size_t>& _indices,
    const std::vector<RELBOOST_FLOAT>& _eta,
    const std::vector<RELBOOST_FLOAT>& _w_fixed,
    const std::vector<RELBOOST_FLOAT>& _yhat_committed ) const
{
    // ------------------------------------------------------------------------

    assert( _eta.size() == targets().size() );
    assert( _w_fixed.size() == targets().size() );
    assert( g_.size() == targets().size() );
    assert( h_.size() == targets().size() );

    assert( sample_weights_ );
    assert( sample_weights_->size() == targets().size() );

    // ------------------------------------------------------------------------
    // Calculate g_eta.

    Eigen::Matrix<RELBOOST_FLOAT, 2, 1> g_eta =
        Eigen::Matrix<RELBOOST_FLOAT, 2, 1>::Zero();

    // The intercept term.
    g_eta[0] = -sum_g_;

    for ( const auto ix : _indices )
        {
            assert( ix < targets().size() );
            g_eta[1] -= g_[ix] * _eta[ix] * sample_weights( ix );
        }

    // ------------------------------------------------------------------------
    // Calculate h_w_const.

    Eigen::Matrix<RELBOOST_FLOAT, 2, 1> h_w_const =
        Eigen::Matrix<RELBOOST_FLOAT, 2, 1>::Zero();

    h_w_const[0] = -sum_h_yhat_committed_;

    for ( const auto ix : _indices )
        {
            assert( !std::isnan( _w_fixed[ix] ) );
            assert( ix < targets().size() );

            h_w_const[0] -= h_[ix] * ( _w_fixed[ix] - _yhat_committed[ix] ) *
                            sample_weights( ix );
            h_w_const[1] -=
                h_[ix] * _w_fixed[ix] * _eta[ix] * sample_weights( ix );
        }

    // ------------------------------------------------------------------------
    // Calculate A.

    Eigen::Matrix<RELBOOST_FLOAT, 2, 2> A =
        Eigen::Matrix<RELBOOST_FLOAT, 2, 2>::Zero();

    // The intercept term.
    A( 0, 0 ) = sum_h_ + hyperparameters().lambda_ *
                             static_cast<RELBOOST_FLOAT>( targets().size() );

    for ( const auto ix : _indices )
        {
            assert( ix < targets().size() );

            A( 0, 1 ) += h_[ix] * _eta[ix] * sample_weights( ix );

            A( 1, 1 ) += ( h_[ix] * _eta[ix] + hyperparameters().lambda_ ) *
                         _eta[ix] * sample_weights( ix );
        }

    // Note that A is symmetric.
    A( 1, 0 ) = A( 0, 1 );

    // ------------------------------------------------------------------------
    // Calculate b.

    const auto b = g_eta + h_w_const;

    // ------------------------------------------------------------------------
    // Calculate weight by solving A*weight = b.

    auto weights = A.fullPivLu().solve( b );

    // ------------------------------------------------------------------------
    // Check relative error.

    const auto relative_error = ( A * weights - b ).norm() / b.norm();

    if ( relative_error > 1e-10 )
        {
            return std::array<RELBOOST_FLOAT, 3>( {NAN, NAN, NAN} );
        }

    // ------------------------------------------------------------------------

    if ( _agg == enums::Aggregation::avg_first_null )
        {
            return std::array<RELBOOST_FLOAT, 3>(
                {weights[0], NAN, weights[1]} );
        }
    else if ( _agg == enums::Aggregation::avg_second_null )
        {
            return std::array<RELBOOST_FLOAT, 3>(
                {weights[0], weights[1], NAN} );
        }
    else
        {
            assert( false && "Aggregation type not known!" );

            return std::array<RELBOOST_FLOAT, 3>( {NAN, NAN, NAN} );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::array<RELBOOST_FLOAT, 3> LossFunctionImpl::calc_weights(
    const RELBOOST_FLOAT _old_weight,
    const std::vector<size_t>& _indices,
    const std::vector<RELBOOST_FLOAT>& _eta1,
    const std::vector<RELBOOST_FLOAT>& _eta2,
    const std::vector<RELBOOST_FLOAT>& _yhat_committed ) const
{
    // ------------------------------------------------------------------------

    assert( _eta1.size() == targets().size() );
    assert( _eta2.size() == targets().size() );
    assert( g_.size() == targets().size() );
    assert( h_.size() == targets().size() );

    assert( sample_weights_ );
    assert( sample_weights_->size() == targets().size() );

    // ------------------------------------------------------------------------
    // Calculate g_eta.

    Eigen::Matrix<RELBOOST_FLOAT, 3, 1> g_eta =
        Eigen::Matrix<RELBOOST_FLOAT, 3, 1>::Zero();

    // The intercept term.
    g_eta[0] = -sum_g_;

    for ( const auto ix : _indices )
        {
            assert( ix < targets().size() );
            g_eta[1] -= g_[ix] * _eta1[ix] * sample_weights( ix );
            g_eta[2] -= g_[ix] * _eta2[ix] * sample_weights( ix );
        }

    // ------------------------------------------------------------------------
    // Calculate h_w_const.

    Eigen::Matrix<RELBOOST_FLOAT, 3, 1> h_w_const =
        Eigen::Matrix<RELBOOST_FLOAT, 3, 1>::Zero();

    h_w_const[0] = -sum_h_yhat_committed_;

    for ( const auto ix : _indices )
        {
            const auto w_old = _old_weight * ( _eta1[ix] + _eta2[ix] );
            const auto w_fixed = _yhat_committed[ix] - w_old;

            assert( ix < targets().size() );
            h_w_const[0] += h_[ix] * w_old * sample_weights( ix );
            h_w_const[1] -= h_[ix] * w_fixed * _eta1[ix] * sample_weights( ix );
            h_w_const[2] -= h_[ix] * w_fixed * _eta2[ix] * sample_weights( ix );
        }

    // ------------------------------------------------------------------------
    // Calculate A.

    Eigen::Matrix<RELBOOST_FLOAT, 3, 3> A =
        Eigen::Matrix<RELBOOST_FLOAT, 3, 3>::Zero();

    // The intercept term.
    A( 0, 0 ) = sum_h_ + hyperparameters().lambda_ *
                             static_cast<RELBOOST_FLOAT>( targets().size() );

    for ( const auto ix : _indices )
        {
            assert( ix < targets().size() );

            A( 0, 1 ) += h_[ix] * _eta1[ix] * sample_weights( ix );

            A( 0, 2 ) += h_[ix] * _eta2[ix] * sample_weights( ix );

            A( 1, 1 ) += ( h_[ix] * _eta1[ix] + hyperparameters().lambda_ ) *
                         _eta1[ix] * sample_weights( ix );

            A( 1, 2 ) += h_[ix] * _eta1[ix] * _eta2[ix] * sample_weights( ix );

            A( 2, 2 ) += ( h_[ix] * _eta2[ix] + hyperparameters().lambda_ ) *
                         _eta2[ix] * sample_weights( ix );
        }

    // Note that A is symmetric.
    A( 1, 0 ) = A( 0, 1 );
    A( 2, 0 ) = A( 0, 2 );
    A( 2, 1 ) = A( 1, 2 );

    // ------------------------------------------------------------------------
    // Calculate b.

    auto b = g_eta + h_w_const;

    // ------------------------------------------------------------------------
    // Calculate weights by solving A*weights = b.

    auto weights = A.fullPivLu().solve( b );

    // ------------------------------------------------------------------------
    // Check relative error.

    const auto relative_error = ( A * weights - b ).norm() / b.norm();

    if ( relative_error > 1e-10 )
        {
            return std::array<RELBOOST_FLOAT, 3>( {NAN, NAN, NAN} );
        }

    // ------------------------------------------------------------------------

    return std::array<RELBOOST_FLOAT, 3>(
        {weights[0], weights[1], weights[2]} );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void LossFunctionImpl::calc_yhat(
    const RELBOOST_FLOAT _old_weight,
    const std::array<RELBOOST_FLOAT, 3>& _new_weights,
    const std::vector<size_t>& _indices,
    const std::vector<RELBOOST_FLOAT>& _eta1,
    const std::vector<RELBOOST_FLOAT>& _eta2,
    const std::vector<RELBOOST_FLOAT>& _yhat_committed,
    std::vector<RELBOOST_FLOAT>* _yhat ) const
{
    for ( auto ix : _indices )
        {
            ( *_yhat )[ix] = _yhat_committed[ix] +
                             _eta1[ix] * std::get<1>( _new_weights ) +
                             _eta2[ix] * std::get<2>( _new_weights ) -
                             ( _eta1[ix] + _eta2[ix] ) * _old_weight;
        }
}

// ----------------------------------------------------------------------------

void LossFunctionImpl::calc_yhat_avg_null(
    const RELBOOST_FLOAT _old_weight,
    const std::array<RELBOOST_FLOAT, 3>& _new_weights,
    const std::vector<size_t>& _indices,
    const std::vector<RELBOOST_FLOAT>& _eta,
    const std::vector<RELBOOST_FLOAT>& _w_fixed,
    std::vector<RELBOOST_FLOAT>* _yhat ) const
{
    if ( std::isnan( std::get<2>( _new_weights ) ) )
        {
            assert( !std::isnan( std::get<1>( _new_weights ) ) );

            for ( auto ix : _indices )
                {
                    ( *_yhat )[ix] =
                        _eta[ix] * std::get<1>( _new_weights ) + _w_fixed[ix];
                }
        }
    else if ( std::isnan( std::get<1>( _new_weights ) ) )
        {
            assert( !std::isnan( std::get<2>( _new_weights ) ) );

            for ( auto ix : _indices )
                {
                    ( *_yhat )[ix] =
                        _eta[ix] * std::get<2>( _new_weights ) + _w_fixed[ix];
                }
        }
    else
        {
            assert(
                false && "Either the first or the second weight must be NAN!" );
        }
}

// ----------------------------------------------------------------------------

RELBOOST_FLOAT LossFunctionImpl::commit(
    const std::vector<size_t>& _indices,
    const std::vector<RELBOOST_FLOAT>& _yhat,
    std::vector<RELBOOST_FLOAT>* _yhat_committed ) const
{
    assert( _yhat_committed->size() == _yhat.size() );
    assert( _yhat_committed->size() == h_.size() );

    RELBOOST_FLOAT sum_h_yhat = sum_h_yhat_committed_;

    for ( size_t ix : _indices )
        {
            assert( ix < _yhat.size() );

            sum_h_yhat += ( _yhat[ix] - ( *_yhat_committed )[ix] ) * h_[ix];
        }

    for ( auto ix : _indices )
        {
            ( *_yhat_committed )[ix] = _yhat[ix];
        }

    return sum_h_yhat;
}
// ----------------------------------------------------------------------------

void LossFunctionImpl::revert_to_commit(
    const std::vector<size_t>& _indices,
    const std::vector<RELBOOST_FLOAT>& _yhat_committed,
    std::vector<RELBOOST_FLOAT>* _yhat ) const
{
    for ( auto ix : _indices )
        {
            ( *_yhat )[ix] = _yhat_committed[ix];
        }
}

// ----------------------------------------------------------------------------

void LossFunctionImpl::transform(
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end,
    const std::vector<RELBOOST_FLOAT>& _weights,
    std::vector<RELBOOST_FLOAT>* _predictions ) const
{
    assert(
        static_cast<int>( _weights.size() ) == std::distance( _begin, _end ) );

    for ( size_t i = 0; i < _weights.size(); ++i )
        {
            const auto it = _begin + i;

            assert( ( *it )->ix_output < _predictions->size() );

            assert( ( *_predictions )[( *it )->ix_output] == 0.0 );

            ( *_predictions )[( *it )->ix_output] = _weights[i];
        }
}

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost
