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

            sum_g2 += g_[ix];
            sum_h2 += h_[ix];
        }

    for ( auto it = _split_begin; it != _split_end; ++it )
        {
            const auto ix = it->ix_output;

            assert_true( ix < g_.size() );

            sum_g1 += g_[ix];
            sum_h1 += h_[ix];
        }

    for ( auto it = _split_end; it != _end; ++it )
        {
            const auto ix = it->ix_output;

            assert_true( ix < g_.size() );

            sum_g2 += g_[ix];
            sum_h2 += h_[ix];
        }

    // ------------------------------------------------------------------------

    n1 = static_cast<Float>( std::distance( _split_begin, _split_end ) );

    n2 = static_cast<Float>( std::distance( _begin, _end ) ) - n1;

    // ------------------------------------------------------------------------

    utils::Reducer::reduce<6>( std::plus<Float>(), _sufficient_stats, _comm );

    *_loss_old = apply_xgboost( sum_g1 + sum_g2, sum_h1 + sum_h2 ).first;

    // ------------------------------------------------------------------------

    const auto m = static_cast<Float>( hyperparameters().min_num_samples_ );

    if ( n1 < m || n2 < m )
        {
            return std::make_pair(
                static_cast<Float>( NAN ),
                std::array<Float, 3>{0.0, 0.0, 0.0} );
        }

    // ------------------------------------------------------------------------

    const auto [loss1, weight1] = apply_xgboost( sum_g1, sum_h1 );

    const auto [loss2, weight2] = apply_xgboost( sum_g2, sum_h2 );

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

    auto n_diff =
        static_cast<Float>( std::distance( _split_begin, _split_end ) );

    utils::Reducer::reduce( std::plus<Float>(), &n_diff, _comm );

    if ( n_diff == 0.0 )
        {
            return std::make_pair(
                static_cast<Float>( NAN ),
                std::array<Float, 3>{0.0, 0.0, 0.0} );
        }

    assert_true( n_diff <= n2 );

    n2 -= n_diff;

    n1 += n_diff;

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

    sum_g2 -= g_diff;
    sum_g1 += g_diff;

    sum_h2 -= h_diff;
    sum_h1 += h_diff;

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

    const auto [loss1, weight1] = apply_xgboost( sum_g1, sum_h1 );

    const auto [loss2, weight2] = apply_xgboost( sum_g2, sum_h2 );

    // ------------------------------------------------------------------------

    const auto loss_reduction = _loss_old - loss1 - loss2;

    const auto weights = std::array<Float, 3>{0.0, weight1, weight2};

    // ------------------------------------------------------------------------

    return std::make_pair( loss_reduction, weights );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------

Float LossFunctionImpl::calc_loss(
    const Float _update_rate,
    const Float _intercept,
    const std::vector<Float>& _predictions,
    multithreading::Communicator* _comm ) const
{
    // ------------------------------------------------------------------------

    assert_true( _predictions.size() == targets().size() );

    // ------------------------------------------------------------------------

    Float loss = 0.0;

    for ( size_t ix : sample_index_ )
        {
            assert_true( ix < _predictions.size() );

            const auto p = _update_rate * ( _intercept + _predictions[ix] );

            loss += ( g_[ix] * p + 0.5 * h_[ix] * p * p ) *
                    ( *sample_weights_ )[ix];
        }

    // ------------------------------------------------------------------------

    utils::Reducer::reduce( std::plus<Float>(), &loss, _comm );

    // ------------------------------------------------------------------------

    if ( sum_sample_weights_ > 0.0 )
        {
            loss /= sum_sample_weights_;
        }

    // ------------------------------------------------------------------------

    return loss;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::pair<Float, std::array<Float, 3>> LossFunctionImpl::calc_pair_avg_null(
    const enums::Aggregation _agg,
    const enums::Update _update,
    const Float _old_weight,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta,
    const std::vector<Float>& _eta_old,
    const std::vector<Float>& _w_fixed,
    const std::vector<Float>& _w_fixed_old,
    const std::vector<Float>& _yhat_committed,
    std::array<Float, 8>* _sufficient_stats,
    multithreading::Communicator* _comm ) const
{
    // ------------------------------------------------------------------------

    calc_sufficient_stats_avg_null(
        _update,
        _old_weight,
        _indices_current,
        _eta,
        _eta_old,
        _w_fixed,
        _w_fixed_old,
        _yhat_committed,
        _sufficient_stats );

    // ------------------------------------------------------------------------
    // Reduce sufficient stats.

    auto sufficient_stats_global = *_sufficient_stats;

    utils::Reducer::reduce<8>(
        std::plus<Float>(), &sufficient_stats_global, _comm );

    const auto g_eta_arr = sufficient_stats_global.data();
    const auto h_w_const_arr = sufficient_stats_global.data() + 2;
    const auto A_arr = sufficient_stats_global.data() + 4;

    // loss_w_fixed is the partial loss incurred by the fixed weights.
    // It can change when weights are set to NULL and is necessary for a
    // fair comparison.
    const auto& loss_w_fixed = *( sufficient_stats_global.data() + 7 );

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

    auto b = g_eta + h_w_const;

    // ------------------------------------------------------------------------
    // Calculate weight by solving A*weight = b.

    Eigen::Matrix<Float, 2, 1> weights = A.fullPivLu().solve( b );

    const auto partial_loss = -0.5 * weights.dot( b ) + loss_w_fixed;

    // ------------------------------------------------------------------------

    auto weights_arr = std::array<Float, 3>{0.0, 0.0, 0.0};

    switch ( _agg )
        {
            case enums::Aggregation::avg_first_null:
                weights_arr =
                    std::array<Float, 3>( {weights[0], NAN, weights[1]} );
                break;

            case enums::Aggregation::avg_second_null:
                weights_arr =
                    std::array<Float, 3>( {weights[0], weights[1], NAN} );
                break;

            default:
                assert_true( false && "Aggregation type not known!" );
        }

    // ------------------------------------------------------------------------

    return std::make_pair( partial_loss, weights_arr );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::pair<Float, std::array<Float, 3>> LossFunctionImpl::calc_pair_non_null(
    const enums::Update _update,
    const Float _old_weight,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta1_old,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _eta2_old,
    const std::vector<Float>& _yhat_committed,
    std::array<Float, 13>* _sufficient_stats,
    multithreading::Communicator* _comm ) const
{
    // ------------------------------------------------------------------------

    calc_sufficient_stats_non_null(
        _update,
        _old_weight,
        _indices_current,
        _eta1,
        _eta1_old,
        _eta2,
        _eta2_old,
        _yhat_committed,
        _sufficient_stats );

    // ------------------------------------------------------------------------
    // Reduce sufficient stats.

    auto sufficient_stats_global = *_sufficient_stats;

    utils::Reducer::reduce<13>(
        std::plus<Float>(), &sufficient_stats_global, _comm );

    const auto g_eta_arr = sufficient_stats_global.data();
    const auto h_w_const_arr = sufficient_stats_global.data() + 3;
    const auto A_arr = sufficient_stats_global.data() + 6;

    // loss_w_fixed is the partial loss incurred by the fixed weights.
    // It can change when weights are set to NULL and is necessary for a
    // fair comparison.
    const auto& loss_w_fixed = *( sufficient_stats_global.data() + 12 );

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

    const Float partial_loss = -0.5 * b.dot( weights ) + loss_w_fixed;

    const auto weights_arr =
        std::array<Float, 3>( {weights[0], weights[1], weights[2]} );

    // ------------------------------------------------------------------------

    return std::make_pair( partial_loss, weights_arr );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Float LossFunctionImpl::calc_regularization_reduction(
    const Float _old_intercept,
    const Float _old_weight,
    const std::array<Float, 3>& _weights ) const
{
    // ------------------------------------------------------------------------

    assert_true( !std::isnan( std::get<0>( _weights ) ) );
    assert_true( !std::isnan( _old_intercept ) );

    // ------------------------------------------------------------------------

    if ( hyperparameters().reg_lambda_ == 0.0 )
        {
            return 0.0;
        }

    // ------------------------------------------------------------------------

    const auto new_intercept = std::get<0>( _weights );
    const auto new_weight1 = std::get<1>( _weights );
    const auto new_weight2 = std::get<2>( _weights );

    Float regularization =
        _old_intercept * _old_intercept - new_intercept * new_intercept;

    if ( std::isnan( new_weight1 ) )
        {
            regularization +=
                _old_weight * _old_weight - new_weight2 * new_weight2;
        }
    else if ( std::isnan( new_weight2 ) )
        {
            regularization +=
                _old_weight * _old_weight - new_weight1 * new_weight1;
        }
    else
        {
            regularization += _old_weight * _old_weight -
                              new_weight1 * new_weight1 -
                              new_weight2 * new_weight2;
        }

    // ------------------------------------------------------------------------

    return 0.5 * hyperparameters().reg_lambda_ * regularization;

    // ------------------------------------------------------------------------
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

void LossFunctionImpl::calc_sufficient_stats_avg_null(
    const enums::Update _update,
    const Float _old_weight,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta,
    const std::vector<Float>& _eta_old,
    const std::vector<Float>& _w_fixed,
    const std::vector<Float>& _w_fixed_old,
    const std::vector<Float>& _yhat_committed,
    std::array<Float, 8>* _sufficient_stats ) const
{
    // ------------------------------------------------------------------------

    assert_true( _eta.size() == targets().size() );
    assert_true( _w_fixed.size() == targets().size() );
    assert_true( _eta_old.size() == targets().size() );
    assert_true( _w_fixed_old.size() == targets().size() );
    assert_true( g_.size() == targets().size() );
    assert_true( h_.size() == targets().size() );

    assert_true( sample_weights_ );
    assert_true( sample_weights_->size() == targets().size() );

    // ------------------------------------------------------------------------

    if ( _update == enums::Update::calc_all )
        {
            std::fill(
                _sufficient_stats->begin(), _sufficient_stats->end(), 0.0 );
        }

    // ------------------------------------------------------------------------

    auto g_eta_arr = _sufficient_stats->data();
    auto h_w_const_arr = _sufficient_stats->data() + 2;
    auto A_arr = _sufficient_stats->data() + 4;

    // loss_w_fixed is the partial loss incurred by the fixed weights.
    // It can change when weights are set to NULL and is necessary for a
    // fair comparison.
    auto& loss_w_fixed = *( _sufficient_stats->data() + 7 );

    // ------------------------------------------------------------------------
    // Calculate g_eta.

    // The intercept term.
    g_eta_arr[0] = -sum_g_;

    for ( const auto ix : _indices_current )
        {
            assert_true( ix < targets().size() );
            assert_true(
                _update != enums::Update::calc_all || _eta_old[ix] == 0.0 );

            const auto d_eta = _eta[ix] - _eta_old[ix];

            g_eta_arr[1] -= g_[ix] * d_eta * sample_weights( ix );
        }

    // ------------------------------------------------------------------------
    // Calculate h_w_const.

    if ( _update == enums::Update::calc_all )
        {
            h_w_const_arr[0] = -sum_h_yhat_committed_;

            for ( const auto ix : _indices_current )
                {
                    h_w_const_arr[0] -= h_[ix] *
                                        ( _w_fixed[ix] - _yhat_committed[ix] ) *
                                        sample_weights( ix );
                }
        }

    for ( const auto ix : _indices_current )
        {
            assert_true( !std::isnan( _w_fixed[ix] ) );
            assert_true( ix < targets().size() );

            const auto d_eta = _eta[ix] - _eta_old[ix];

            const auto d_w_fixed = _w_fixed[ix] - _w_fixed_old[ix];

            const auto d_w_fixed2 = _w_fixed[ix] * _w_fixed[ix] -
                                    _w_fixed_old[ix] * _w_fixed_old[ix];

            h_w_const_arr[1] -=
                h_[ix] * d_w_fixed * d_eta * sample_weights( ix );

            loss_w_fixed += ( g_[ix] * d_w_fixed + 0.5 * h_[ix] * d_w_fixed2 ) *
                            sample_weights( ix );
        }

    // ------------------------------------------------------------------------
    // Calculate A.

    if ( _update == enums::Update::calc_all )
        {
            A_arr[0] = sum_h_ + hyperparameters().reg_lambda_;  // A( 0, 0 )

            A_arr[2] = hyperparameters().reg_lambda_;  // A( 1, 1 )
        }

    for ( const auto ix : _indices_current )
        {
            assert_true( ix < targets().size() );

            const auto d_eta = _eta[ix] - _eta_old[ix];

            const auto d_eta2 =
                _eta[ix] * _eta[ix] - _eta_old[ix] * _eta_old[ix];

            A_arr[1] += h_[ix] * d_eta * sample_weights( ix );  // A( 0, 1 )

            A_arr[2] += h_[ix] * d_eta2 * sample_weights( ix );  // A( 1, 1 )
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void LossFunctionImpl::calc_sufficient_stats_non_null(
    const enums::Update _update,
    const Float _old_weight,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta1_old,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _eta2_old,
    const std::vector<Float>& _yhat_committed,
    std::array<Float, 13>* _sufficient_stats ) const
{
    // ------------------------------------------------------------------------

    assert_true( _eta1.size() == targets().size() );
    assert_true( _eta2.size() == targets().size() );
    assert_true( _eta1_old.size() == targets().size() );
    assert_true( _eta2_old.size() == targets().size() );
    assert_true( g_.size() == targets().size() );
    assert_true( h_.size() == targets().size() );

    assert_true( sample_weights_ );
    assert_true( sample_weights_->size() == targets().size() );

    // ------------------------------------------------------------------------

    if ( _update == enums::Update::calc_all )
        {
            std::fill(
                _sufficient_stats->begin(), _sufficient_stats->end(), 0.0 );
        }

    // ------------------------------------------------------------------------

    auto g_eta_arr = _sufficient_stats->data();
    auto h_w_const_arr = _sufficient_stats->data() + 3;
    auto A_arr = _sufficient_stats->data() + 6;

    // loss_w_fixed is the partial loss incurred by the fixed weights.
    // It can change when weights are set to NULL and is necessary for a
    // fair comparison.
    auto& loss_w_fixed = *( _sufficient_stats->data() + 12 );

    // ------------------------------------------------------------------------
    // Calculate g_eta_arr.

    // The intercept term.
    g_eta_arr[0] = -sum_g_;

    for ( const auto ix : _indices_current )
        {
            assert_true( ix < targets().size() );

            const auto d_eta1 = _eta1[ix] - _eta1_old[ix];
            const auto d_eta2 = _eta2[ix] - _eta2_old[ix];

            g_eta_arr[1] -= g_[ix] * d_eta1 * sample_weights( ix );
            g_eta_arr[2] -= g_[ix] * d_eta2 * sample_weights( ix );
        }

    // ------------------------------------------------------------------------
    // Calculate h_w_const_arr and loss_w_fixed.
    // NOTE: w_fixed = yhat_committed - impact of old weight.

    // The intercept term
    if ( _update == enums::Update::calc_all )
        {
            h_w_const_arr[0] = -sum_h_yhat_committed_;

            for ( const auto ix : _indices_current )
                {
                    assert_true( _eta1_old[ix] == 0.0 );
                    assert_true( _eta2_old[ix] == 0.0 );

                    const auto w_old = _old_weight * ( _eta1[ix] + _eta2[ix] );
                    const auto w_fixed = _yhat_committed[ix] - w_old;

                    h_w_const_arr[0] += h_[ix] * w_old * sample_weights( ix );

                    loss_w_fixed += ( g_[ix] + 0.5 * h_[ix] * w_fixed ) *
                                    w_fixed * sample_weights( ix );
                }
        }

    for ( const auto ix : _indices_current )
        {
            const auto w_old = _old_weight * ( _eta1[ix] + _eta2[ix] );
            const auto w_fixed = _yhat_committed[ix] - w_old;

            const auto d_eta1 = _eta1[ix] - _eta1_old[ix];
            const auto d_eta2 = _eta2[ix] - _eta2_old[ix];

            h_w_const_arr[1] -=
                h_[ix] * w_fixed * d_eta1 * sample_weights( ix );
            h_w_const_arr[2] -=
                h_[ix] * w_fixed * d_eta2 * sample_weights( ix );
        }

    // ------------------------------------------------------------------------
    // Calculate A_arr.

    if ( _update == enums::Update::calc_all )
        {
            A_arr[0] = sum_h_ + hyperparameters().reg_lambda_;  // A( 0, 0 )
            A_arr[3] = hyperparameters().reg_lambda_;           // A( 1, 1 )
            A_arr[5] = hyperparameters().reg_lambda_;           // A( 2, 2 )
        }

    for ( const auto ix : _indices_current )
        {
            assert_true( ix < targets().size() );

            const auto d_eta1 = _eta1[ix] - _eta1_old[ix];

            const auto d_eta2 = _eta2[ix] - _eta2_old[ix];

            const auto d_eta11 =
                _eta1[ix] * _eta1[ix] - _eta1_old[ix] * _eta1_old[ix];

            const auto d_eta22 =
                _eta2[ix] * _eta2[ix] - _eta2_old[ix] * _eta2_old[ix];

            const auto d_eta12 =
                _eta1[ix] * _eta2[ix] - _eta1_old[ix] * _eta2_old[ix];

            A_arr[1] += h_[ix] * d_eta1 * sample_weights( ix );  // A( 0, 1 )

            A_arr[2] += h_[ix] * d_eta2 * sample_weights( ix );  //  A( 0, 2 )

            A_arr[3] += h_[ix] * d_eta11 * sample_weights( ix );  // A( 1, 1 )

            A_arr[4] += h_[ix] * d_eta12 * sample_weights( ix );  // A( 1, 2 )

            A_arr[5] += h_[ix] * d_eta22 * sample_weights( ix );  // A( 2, 2 )
        }
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

void LossFunctionImpl::revert( std::array<Float, 6>* _sufficient_stats ) const
{
    auto& sum_g1 = std::get<0>( *_sufficient_stats );
    auto& sum_h1 = std::get<1>( *_sufficient_stats );
    auto& sum_g2 = std::get<2>( *_sufficient_stats );
    auto& sum_h2 = std::get<3>( *_sufficient_stats );
    auto& n1 = std::get<4>( *_sufficient_stats );
    auto& n2 = std::get<5>( *_sufficient_stats );

    sum_g2 += sum_g1;
    sum_g1 = 0.0;

    sum_h2 += sum_h1;
    sum_h1 = 0.0;

    n2 += n1;
    n1 = 0.0;
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
