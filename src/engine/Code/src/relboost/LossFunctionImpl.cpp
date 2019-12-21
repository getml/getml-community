#include "relboost/lossfunctions/lossfunctions.hpp"

namespace relboost
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

std::pair<Float, std::array<Float, 3>> LossFunctionImpl::calc_all(
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _split_begin,
    const std::vector<containers::Match>::iterator _split_end,
    const std::vector<containers::Match>::iterator _end,
    Float* _loss_old,
    std::array<Float, 6>* _sufficient_stats,
    multithreading::Communicator* _comm ) const
{
    // ------------------------------------------------------------------------

    assert_true( _begin <= _split_begin );
    assert_true( _split_begin <= _split_end );
    assert_true( _split_end <= _end );

    assert_true( g_.size() == h_.size() );

    assert_true( _begin == _end || g_.size() > 0 );

    auto& sum_g1 = std::get<0>( *_sufficient_stats );
    auto& sum_h1 = std::get<1>( *_sufficient_stats );
    auto& sum_g2 = std::get<2>( *_sufficient_stats );
    auto& sum_h2 = std::get<3>( *_sufficient_stats );
    auto& n1 = std::get<4>( *_sufficient_stats );
    auto& n2 = std::get<5>( *_sufficient_stats );

    // ------------------------------------------------------------------------

    *_sufficient_stats = std::array<Float, 6>{0.0, 0.0, 0.0, 0.0, 0.0, 0.0};

    // ------------------------------------------------------------------------

    for ( auto it = _begin; it != _split_begin; ++it )
        {
            const auto ix = it->ix_output;

            assert_true( ix < g_.size() );

            sum_g1 += g_[ix];
            sum_h1 += h_[ix];
        }

    for ( auto it = _split_begin; it != _split_end; ++it )
        {
            const auto ix = it->ix_output;

            assert_true( ix < g_.size() );

            sum_g2 += g_[ix];
            sum_h2 += h_[ix];
        }

    for ( auto it = _split_end; it != _end; ++it )
        {
            const auto ix = it->ix_output;

            assert_true( ix < g_.size() );

            sum_g1 += g_[ix];
            sum_h1 += h_[ix];
        }

    // ------------------------------------------------------------------------

    n2 = static_cast<Float>( std::distance( _split_begin, _split_end ) );

    n1 = static_cast<Float>( std::distance( _begin, _end ) ) - n2;

    // ------------------------------------------------------------------------

    utils::Reducer::reduce<6>( std::plus<Float>(), _sufficient_stats, _comm );

    *_loss_old =
        apply_xgboost( sum_g1 + sum_g2, sum_h1 + sum_h2, n1 + n2 ).first;

    // ------------------------------------------------------------------------

    const auto m = static_cast<Float>( hyperparameters().min_num_samples_ );

    if ( n1 < m || n2 < m )
        {
            return std::make_pair(
                static_cast<Float>( NAN ),
                std::array<Float, 3>{0.0, 0.0, 0.0} );
        }

    // ------------------------------------------------------------------------

    const auto [loss1, weight1] = apply_xgboost( sum_g1, sum_h1, n1 );

    const auto [loss2, weight2] = apply_xgboost( sum_g2, sum_h2, n2 );

    const auto loss_reduction = *_loss_old - loss1 - loss2;

    const auto weights = std::array<Float, 3>{0.0, weight1, weight2};

    // ------------------------------------------------------------------------

    return std::make_pair( loss_reduction, weights );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::pair<Float, std::array<Float, 3>> LossFunctionImpl::calc_diff(
    const enums::Revert _revert,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _split_begin,
    const std::vector<containers::Match>::iterator _split_end,
    const std::vector<containers::Match>::iterator _end,
    const Float _loss_old,
    std::array<Float, 6>* _sufficient_stats,
    multithreading::Communicator* _comm ) const
{
    // ------------------------------------------------------------------------

    assert_true( _begin <= _split_begin );
    assert_true( _split_begin <= _split_end );
    assert_true( _split_end <= _end );

    assert_true( g_.size() == h_.size() );

    auto& sum_g1 = std::get<0>( *_sufficient_stats );
    auto& sum_h1 = std::get<1>( *_sufficient_stats );
    auto& sum_g2 = std::get<2>( *_sufficient_stats );
    auto& sum_h2 = std::get<3>( *_sufficient_stats );
    auto& n1 = std::get<4>( *_sufficient_stats );
    auto& n2 = std::get<5>( *_sufficient_stats );

    // ------------------------------------------------------------------------

    auto n2_diff =
        static_cast<Float>( std::distance( _split_begin, _split_end ) );

    utils::Reducer::reduce( std::plus<Float>(), &n2_diff, _comm );

    assert_true( n2_diff <= n1 );

    n1 -= n2_diff;

    n2 += n2_diff;

    // ------------------------------------------------------------------------

    if ( _revert == enums::Revert::True )
        {
            const auto m =
                static_cast<Float>( hyperparameters().min_num_samples_ );

            if ( n1 < m || n2 < m )
                {
                    return std::make_pair(
                        static_cast<Float>( NAN ),
                        std::array<Float, 3>{0.0, 0.0, 0.0} );
                }
        }

    // ------------------------------------------------------------------------

    auto g_h_diff = std::array<Float, 2>{0.0, 0.0};

    auto& g_diff = std::get<0>( g_h_diff );
    auto& h_diff = std::get<1>( g_h_diff );

    for ( auto it = _split_begin; it != _split_end; ++it )
        {
            const auto ix = it->ix_output;

            assert_true( ix < g_.size() );

            g_diff += g_[ix];
            h_diff += h_[ix];
        }

    utils::Reducer::reduce<2>( std::plus<Float>(), &g_h_diff, _comm );

    sum_g1 -= g_diff;
    sum_g2 += g_diff;

    sum_h1 -= h_diff;
    sum_h2 += h_diff;

    // ------------------------------------------------------------------------

    if ( _revert == enums::Revert::False )
        {
            const auto m =
                static_cast<Float>( hyperparameters().min_num_samples_ );

            if ( n1 < m || n2 < m )
                {
                    return std::make_pair(
                        static_cast<Float>( NAN ),
                        std::array<Float, 3>{0.0, 0.0, 0.0} );
                }
        }

    // ------------------------------------------------------------------------

    const auto [loss1, weight1] = apply_xgboost( sum_g1, sum_h1, n1 );

    const auto [loss2, weight2] = apply_xgboost( sum_g2, sum_h2, n2 );

    // ------------------------------------------------------------------------

    const auto loss_reduction = _loss_old - loss1 - loss2;

    const auto weights = std::array<Float, 3>{0.0, weight1, weight2};

    // ------------------------------------------------------------------------

    return std::make_pair( loss_reduction, weights );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::pair<Float, std::array<Float, 3>> LossFunctionImpl::calc_pair(
    const enums::Revert _revert,
    const enums::Update _update,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _split_begin,
    const std::vector<containers::Match>::iterator _split_end,
    const std::vector<containers::Match>::iterator _end,
    Float* _loss_old,
    std::array<Float, 6>* _sufficient_stats,
    multithreading::Communicator* _comm ) const
{
    switch ( _update )
        {
            case enums::Update::calc_all:
                return calc_all(
                    _begin,
                    _split_begin,
                    _split_end,
                    _end,
                    _loss_old,
                    _sufficient_stats,
                    _comm );
                break;

            case enums::Update::calc_diff:
                return calc_diff(
                    _revert,
                    _begin,
                    _split_begin,
                    _split_end,
                    _end,
                    *_loss_old,
                    _sufficient_stats,
                    _comm );
                break;

            default:
                assert_true( false && "Unknown update!" );
                return std::make_pair( 0.0, std::array<Float, 3>() );
        }
}

// ----------------------------------------------------------------------------

Float LossFunctionImpl::calc_regularization_reduction(
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta2,
    const std::vector<size_t>& _indices,
    const Float _old_intercept,
    const Float _old_weight,
    const std::array<Float, 3>& _weights,
    const Float _sum_sample_weights,
    multithreading::Communicator* _comm ) const
{
    // ------------------------------------------------------------------------

    assert_true( !std::isnan( std::get<0>( _weights ) ) );
    assert_true( !std::isnan( _old_intercept ) );

    assert_true( _eta1.size() == _eta2.size() );
    assert_true( _eta1.size() == targets().size() );

    assert_true( sample_weights_ );
    assert_true( _eta1.size() == sample_weights_->size() );

    // ------------------------------------------------------------------------

    if ( hyperparameters().reg_lambda_ == 0.0 )
        {
            return 0.0;
        }

    // ------------------------------------------------------------------------

    Float regularization = 0.0;

    if ( std::isnan( std::get<1>( _weights ) ) )
        {
            // In the case of of the weights being nan, _eta2 plays the role
            // of _eta_old and _eta1 plays _eta_new.

            regularization += calc_regularization_reduction(
                _eta2, _eta1, _indices, _old_weight, std::get<2>( _weights ) );
        }
    else if ( std::isnan( std::get<2>( _weights ) ) )
        {
            // In the case of of the weights being nan, _eta2 plays the role
            // of _eta_old and _eta1 plays _eta_new.

            regularization += calc_regularization_reduction(
                _eta2, _eta1, _indices, _old_weight, std::get<1>( _weights ) );
        }
    else
        {
            for ( size_t ix : _indices )
                {
                    assert_true( ix < targets().size() );
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

    // ------------------------------------------------------------------------

    utils::Reducer::reduce( std::plus<Float>(), &regularization, _comm );

    // ------------------------------------------------------------------------

    regularization /= _sum_sample_weights;

    regularization +=
        ( _old_intercept * _old_intercept -
          std::get<0>( _weights ) * std::get<0>( _weights ) );

    // ------------------------------------------------------------------------

    return 0.5 * hyperparameters().reg_lambda_ * regularization;
}

// ----------------------------------------------------------------------------

Float LossFunctionImpl::calc_regularization_reduction(
    const std::vector<Float>& _eta_old,
    const std::vector<Float>& _eta_new,
    const std::vector<size_t>& _indices,
    const Float _old_weight,
    const Float _new_weight ) const
{
    assert_true( _eta_old.size() == targets().size() );
    assert_true( _eta_new.size() == targets().size() );

    assert_true( sample_weights_ );
    assert_true( sample_weights_->size() == targets().size() );

    Float regularization = 0.0;

    if ( std::isnan( _old_weight ) )
        {
            for ( size_t ix : _indices )
                {
                    assert_true( ix < targets().size() );
                    regularization -=
                        sample_weights( ix ) *
                        ( _new_weight * _new_weight * _eta_new[ix] );
                }
        }
    else
        {
            for ( size_t ix : _indices )
                {
                    assert_true( ix < targets().size() );
                    regularization +=
                        sample_weights( ix ) *
                        ( _old_weight * _old_weight * _eta_old[ix] -
                          _new_weight * _new_weight * _eta_new[ix] );
                }
        }

    return regularization;
}

// ----------------------------------------------------------------------------

std::vector<size_t> LossFunctionImpl::calc_sample_index(
    const std::shared_ptr<const std::vector<Float>>& _sample_weights ) const
{
    assert_true( _sample_weights );

    auto sample_index = std::vector<size_t>( 0 );

    for ( size_t i = 0; i < _sample_weights->size(); ++i )
        {
            if ( ( *_sample_weights )[i] > 0.0 )
                {
                    sample_index.push_back( i );
                }
        }

    return sample_index;
}

// ----------------------------------------------------------------------------

void LossFunctionImpl::calc_sums(
    const std::vector<size_t>& _sample_index,
    const std::vector<Float>& _sample_weights,
    Float* _sum_g,
    Float* _sum_h,
    Float* _sum_sample_weights,
    multithreading::Communicator* _comm ) const
{
    // ------------------------------------------------------------------------

    assert_true( g_.size() == _sample_weights.size() );
    assert_true( h_.size() == _sample_weights.size() );

    // ------------------------------------------------------------------------

    *_sum_g = 0.0;

    for ( auto ix : _sample_index )
        {
            assert_true( ix < _sample_weights.size() );
            *_sum_g += g_[ix] * _sample_weights[ix];
        }

    // ------------------------------------------------------------------------

    *_sum_h = 0.0;

    for ( auto ix : _sample_index )
        {
            assert_true( ix < _sample_weights.size() );
            *_sum_h += h_[ix] * _sample_weights[ix];
        }

    // ------------------------------------------------------------------------

    *_sum_sample_weights = 0.0;

    for ( auto ix : _sample_index )
        {
            assert_true( ix < _sample_weights.size() );
            *_sum_sample_weights += _sample_weights[ix];
        }

    // ------------------------------------------------------------------------

    utils::Reducer::reduce( std::plus<Float>(), _sum_sample_weights, _comm );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Float LossFunctionImpl::calc_update_rate(
    const std::vector<Float>& _yhat_old,
    const std::vector<Float>& _predictions,
    multithreading::Communicator* _comm ) const
{
    // ------------------------------------------------------------------------

    assert_true( _yhat_old.size() == _predictions.size() );
    assert_true( _yhat_old.size() == targets().size() );

    assert_true( _yhat_old.size() == g_.size() );
    assert_true( _yhat_old.size() == h_.size() );

    // ------------------------------------------------------------------------

    Float sum_g_predictions = 0.0;

    for ( size_t i = 0; i < _predictions.size(); ++i )
        {
            sum_g_predictions += g_[i] * _predictions[i];
        }

    // ------------------------------------------------------------------------

    Float sum_h_predictions = 0.0;

    for ( size_t i = 0; i < _predictions.size(); ++i )
        {
            sum_h_predictions += h_[i] * _predictions[i] * _predictions[i];
        }

    // ------------------------------------------------------------------------

    utils::Reducer::reduce( std::plus<Float>(), &sum_g_predictions, _comm );

    utils::Reducer::reduce( std::plus<Float>(), &sum_h_predictions, _comm );

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

std::pair<Float, std::array<Float, 3>> LossFunctionImpl::calc_weights_avg_null(
    const enums::Aggregation _agg,
    const Float _old_weight,
    const std::vector<size_t>& _indices,
    const std::vector<Float>& _eta,
    const std::vector<Float>& _w_fixed,
    const std::vector<Float>& _yhat_committed,
    multithreading::Communicator* _comm ) const
{
    // ------------------------------------------------------------------------

    assert_true( _eta.size() == targets().size() );
    assert_true( _w_fixed.size() == targets().size() );
    assert_true( g_.size() == targets().size() );
    assert_true( h_.size() == targets().size() );

    assert_true( sample_weights_ );
    assert_true( sample_weights_->size() == targets().size() );

    // ------------------------------------------------------------------------
    // Calculate g_eta.

    std::array<Float, 2> g_eta_arr = {0.0, 0.0};

    // The intercept term.
    g_eta_arr[0] = -sum_g_;

    for ( const auto ix : _indices )
        {
            assert_true( ix < targets().size() );
            g_eta_arr[1] -= g_[ix] * _eta[ix] * sample_weights( ix );
        }

    // ------------------------------------------------------------------------
    // Calculate h_w_const.

    std::array<Float, 2> h_w_const_arr = {0.0, 0.0};

    h_w_const_arr[0] = -sum_h_yhat_committed_;

    for ( const auto ix : _indices )
        {
            assert_true( !std::isnan( _w_fixed[ix] ) );
            assert_true( ix < targets().size() );

            h_w_const_arr[0] -= h_[ix] *
                                ( _w_fixed[ix] - _yhat_committed[ix] ) *
                                sample_weights( ix );
            h_w_const_arr[1] -=
                h_[ix] * _w_fixed[ix] * _eta[ix] * sample_weights( ix );
        }

    // ------------------------------------------------------------------------
    // Calculate A.

    std::array<Float, 3> A_arr = {0.0, 0.0, 0.0};

    // The intercept term.
    A_arr[0] =
        sum_h_ + hyperparameters().reg_lambda_ *
                     static_cast<Float>( targets().size() );  // A( 0, 0 )

    for ( const auto ix : _indices )
        {
            assert_true( ix < targets().size() );

            A_arr[1] += h_[ix] * _eta[ix] * sample_weights( ix );  // A( 0, 1 )

            A_arr[2] += ( h_[ix] * _eta[ix] + hyperparameters().reg_lambda_ ) *
                        _eta[ix] * sample_weights( ix );  // A( 1, 1 )
        }

    // ------------------------------------------------------------------------
    // Reduce.

    utils::Reducer::reduce<2>( std::plus<Float>(), &g_eta_arr, _comm );

    utils::Reducer::reduce<2>( std::plus<Float>(), &h_w_const_arr, _comm );

    utils::Reducer::reduce<3>( std::plus<Float>(), &A_arr, _comm );

    // ------------------------------------------------------------------------
    // Transfer data to Eigen::Matrix.

    Eigen::Matrix<Float, 2, 1> g_eta;
    g_eta[0] = g_eta_arr[0];
    g_eta[1] = g_eta_arr[1];

    Eigen::Matrix<Float, 2, 1> h_w_const;
    h_w_const[0] = h_w_const_arr[0];
    h_w_const[1] = h_w_const_arr[1];

    Eigen::Matrix<Float, 2, 2> A;
    A( 0, 0 ) = A_arr[0];
    A( 1, 1 ) = A_arr[2];
    A( 1, 0 ) = A( 0, 1 ) = A_arr[1];

    // ------------------------------------------------------------------------
    // Calculate b.

    const auto b = g_eta + h_w_const;

    // ------------------------------------------------------------------------
    // Calculate weight by solving A*weight = b.

    Eigen::Matrix<Float, 2, 1> weights = A.fullPivLu().solve( b );

    // TODO: Missing loss of old weight!
    const auto loss_reduction = 0.5 * weights.dot( b );

    // ------------------------------------------------------------------------

    auto weights_arr = std::array<Float, 3>{0.0, 0.0, 0.0};

    if ( _agg == enums::Aggregation::avg_first_null )
        {
            weights_arr = std::array<Float, 3>( {weights[0], NAN, weights[1]} );
        }
    else if ( _agg == enums::Aggregation::avg_second_null )
        {
            weights_arr = std::array<Float, 3>( {weights[0], weights[1], NAN} );
        }
    else
        {
            assert_true( false && "Aggregation type not known!" );
        }

    // ------------------------------------------------------------------------

    return std::make_pair( loss_reduction, weights_arr );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::pair<Float, std::array<Float, 3>> LossFunctionImpl::calc_weights(
    const Float _old_intercept,
    const Float _old_weight,
    const std::vector<size_t>& _indices,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _yhat_committed,
    multithreading::Communicator* _comm ) const
{
    // ------------------------------------------------------------------------

    assert_true( _eta1.size() == targets().size() );
    assert_true( _eta2.size() == targets().size() );
    assert_true( g_.size() == targets().size() );
    assert_true( h_.size() == targets().size() );

    assert_true( sample_weights_ );
    assert_true( sample_weights_->size() == targets().size() );

    // ------------------------------------------------------------------------
    // Calculate g_eta_arr.

    std::array<Float, 3> g_eta_arr = {0.0, 0.0, 0.0};

    // The intercept term.
    g_eta_arr[0] = -sum_g_;

    for ( const auto ix : _indices )
        {
            assert_true( ix < targets().size() );
            g_eta_arr[1] -= g_[ix] * _eta1[ix] * sample_weights( ix );
            g_eta_arr[2] -= g_[ix] * _eta2[ix] * sample_weights( ix );
        }

    // ------------------------------------------------------------------------
    // Calculate h_w_const_arr.

    std::array<Float, 3> h_w_const_arr = {0.0, 0.0, 0.0};

    h_w_const_arr[0] = -sum_h_yhat_committed_;

    for ( const auto ix : _indices )
        {
            const auto w_old = _old_weight * ( _eta1[ix] + _eta2[ix] );
            const auto w_fixed = _yhat_committed[ix] - w_old;

            assert_true( sample_weights( ix ) > 0.0 );

            h_w_const_arr[0] += h_[ix] * w_old * sample_weights( ix );
            h_w_const_arr[1] -=
                h_[ix] * w_fixed * _eta1[ix] * sample_weights( ix );
            h_w_const_arr[2] -=
                h_[ix] * w_fixed * _eta2[ix] * sample_weights( ix );
        }

    // ------------------------------------------------------------------------
    // Calculate A_arr.

    std::array<Float, 6> A_arr = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};

    // The intercept term.
    A_arr[0] =
        sum_h_ + hyperparameters().reg_lambda_ *
                     static_cast<Float>( targets().size() );  // A( 0, 0 )

    for ( const auto ix : _indices )
        {
            assert_true( ix < targets().size() );

            A_arr[1] += h_[ix] * _eta1[ix] * sample_weights( ix );  // A( 0, 1 )

            A_arr[2] +=
                h_[ix] * _eta2[ix] * sample_weights( ix );  //  A( 0, 2 )

            A_arr[3] += ( h_[ix] * _eta1[ix] + hyperparameters().reg_lambda_ ) *
                        _eta1[ix] * sample_weights( ix );  // A( 1, 1 )

            A_arr[4] += h_[ix] * _eta1[ix] * _eta2[ix] *
                        sample_weights( ix );  // A( 1, 2 )

            A_arr[5] += ( h_[ix] * _eta2[ix] + hyperparameters().reg_lambda_ ) *
                        _eta2[ix] * sample_weights( ix );  // A( 2, 2 )
        }

    // ------------------------------------------------------------------------
    // Reduce.

    utils::Reducer::reduce<3>( std::plus<Float>(), &g_eta_arr, _comm );

    utils::Reducer::reduce<3>( std::plus<Float>(), &h_w_const_arr, _comm );

    utils::Reducer::reduce<6>( std::plus<Float>(), &A_arr, _comm );

    // ------------------------------------------------------------------------
    // Transfer data to Eigen::Matrix.

    Eigen::Matrix<Float, 3, 1> g_eta;
    g_eta[0] = g_eta_arr[0];
    g_eta[1] = g_eta_arr[1];
    g_eta[2] = g_eta_arr[2];

    Eigen::Matrix<Float, 3, 1> h_w_const;
    h_w_const[0] = h_w_const_arr[0];
    h_w_const[1] = h_w_const_arr[1];
    h_w_const[2] = h_w_const_arr[2];

    Eigen::Matrix<Float, 3, 3> A;
    A( 0, 0 ) = A_arr[0];
    A( 1, 1 ) = A_arr[3];
    A( 2, 2 ) = A_arr[5];
    A( 1, 0 ) = A( 0, 1 ) = A_arr[1];
    A( 2, 0 ) = A( 0, 2 ) = A_arr[2];
    A( 2, 1 ) = A( 1, 2 ) = A_arr[4];

    // ------------------------------------------------------------------------
    // Calculate b.

    auto b = g_eta + h_w_const;

    // ------------------------------------------------------------------------
    // Calculate weights by solving A*weights = b.

    const auto weights = A.fullPivLu().solve( b );

    // ------------------------------------------------------------------------

    const Float loss_old =
        -0.5 * ( _old_intercept * b[0] + _old_weight * ( b[1] + b[2] ) );

    const Float loss_new = -0.5 * b.dot( weights );

    const auto loss_reduction = loss_old - loss_new;

    const auto weights_arr =
        std::array<Float, 3>( {weights[0], weights[1], weights[2]} );

    // ------------------------------------------------------------------------

    return std::make_pair( loss_reduction, weights_arr );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void LossFunctionImpl::calc_yhat(
    const Float _old_weight,
    const std::array<Float, 3>& _new_weights,
    const std::vector<size_t>& _indices,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _yhat_committed,
    std::vector<Float>* _yhat ) const
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
    const Float _old_weight,
    const std::array<Float, 3>& _new_weights,
    const std::vector<size_t>& _indices,
    const std::vector<Float>& _eta,
    const std::vector<Float>& _w_fixed,
    std::vector<Float>* _yhat ) const
{
    if ( std::isnan( std::get<2>( _new_weights ) ) )
        {
            assert_true( !std::isnan( std::get<1>( _new_weights ) ) );

            for ( auto ix : _indices )
                {
                    ( *_yhat )[ix] =
                        _eta[ix] * std::get<1>( _new_weights ) + _w_fixed[ix];
                }
        }
    else if ( std::isnan( std::get<1>( _new_weights ) ) )
        {
            assert_true( !std::isnan( std::get<2>( _new_weights ) ) );

            for ( auto ix : _indices )
                {
                    ( *_yhat )[ix] =
                        _eta[ix] * std::get<2>( _new_weights ) + _w_fixed[ix];
                }
        }
    else
        {
            assert_true(
                false && "Either the first or the second weight must be NAN!" );
        }
}

// ----------------------------------------------------------------------------

Float LossFunctionImpl::commit(
    const std::vector<size_t>& _indices,
    const std::vector<Float>& _yhat,
    std::vector<Float>* _yhat_committed ) const
{
    assert_true( _yhat_committed->size() == _yhat.size() );
    assert_true( _yhat_committed->size() == h_.size() );

    Float sum_h_yhat = sum_h_yhat_committed_;

    for ( size_t ix : _indices )
        {
            assert_true( ix < _yhat.size() );

            sum_h_yhat += sample_weights( ix ) *
                          ( _yhat[ix] - ( *_yhat_committed )[ix] ) * h_[ix];
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
    const std::vector<Float>& _yhat_committed,
    std::vector<Float>* _yhat ) const
{
    for ( auto ix : _indices )
        {
            ( *_yhat )[ix] = _yhat_committed[ix];
        }
}

// ----------------------------------------------------------------------------

void LossFunctionImpl::transform(
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    const std::vector<Float>& _weights,
    std::vector<Float>* _predictions ) const
{
    assert_true(
        static_cast<int>( _weights.size() ) == std::distance( _begin, _end ) );

    for ( size_t i = 0; i < _weights.size(); ++i )
        {
            const auto it = _begin + i;

            assert_true( it->ix_output < _predictions->size() );

            assert_true( ( *_predictions )[it->ix_output] == 0.0 );

            ( *_predictions )[it->ix_output] = _weights[i];
        }
}

// ----------------------------------------------------------------------------

void LossFunctionImpl::update_yhat_old(
    const Float _update_rate,
    const std::vector<Float>& _predictions,
    std::vector<Float>* _yhat_old ) const
{
    assert_true( _predictions.size() == _yhat_old->size() );

    std::transform(
        _yhat_old->begin(),
        _yhat_old->end(),
        _predictions.begin(),
        _yhat_old->begin(),
        [_update_rate]( const Float yhat, const Float pred ) {
            return yhat + pred * _update_rate;
        } );
}

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost
