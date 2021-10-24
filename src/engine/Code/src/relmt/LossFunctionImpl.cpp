#include "relmt/lossfunctions/lossfunctions.hpp"

namespace relmt
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

std::pair<Float, containers::Weights> LossFunctionImpl::calc_all(
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _split_begin,
    const std::vector<containers::Match>::iterator _split_end,
    const std::vector<containers::Match>::iterator _end,
    Float* _loss_old,
    std::array<Float, 6>* _sufficient_stats,
    multithreading::Communicator* _comm ) const
{
    // TODO
    return std::make_pair( 0.0, containers::Weights() );
}

// ----------------------------------------------------------------------------

std::pair<Float, containers::Weights> LossFunctionImpl::calc_diff(
    const enums::Revert _revert,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _split_begin,
    const std::vector<containers::Match>::iterator _split_end,
    const std::vector<containers::Match>::iterator _end,
    const Float _loss_old,
    std::array<Float, 6>* _sufficient_stats,
    multithreading::Communicator* _comm ) const
{
    return std::make_pair( 0.0, containers::Weights() );
}

// ----------------------------------------------------------------------------

void LossFunctionImpl::calc_g_ptr(
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta1_old,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _eta2_old,
    Float* _g_ptr ) const
{
    assert_true( nrows_ > 0 );
    assert_true( _eta1.size() % nrows_ == 0 );

    const auto ncolsplus1 = _eta1.size() / nrows_;

    // The intercept term.
    _g_ptr[0] = -sum_g_;

    for ( const auto ix : _indices_current )
        {
            assert_true( ix < nrows_ );

            const auto g_sample_weights = g_[ix] * sample_weights( ix );

            for ( size_t j = 0; j < ncolsplus1; ++j )
                {
                    const auto d_eta = _eta1[ix * ncolsplus1 + j] -
                                       _eta1_old[ix * ncolsplus1 + j];

                    assert_true( !std::isnan( d_eta ) && !std::isinf( d_eta ) );

                    _g_ptr[j + 1] -= g_sample_weights * d_eta;
                }

            for ( size_t j = 0; j < ncolsplus1; ++j )
                {
                    const auto d_eta = _eta2[ix * ncolsplus1 + j] -
                                       _eta2_old[ix * ncolsplus1 + j];

                    assert_true( !std::isnan( d_eta ) && !std::isinf( d_eta ) );

                    _g_ptr[ncolsplus1 + j + 1] -= g_sample_weights * d_eta;
                }
        }
}

// ----------------------------------------------------------------------------

void LossFunctionImpl::calc_Hfwf_ptr(
    const enums::Update _update,
    const std::vector<Float>& _old_weights,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta1_old,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _eta2_old,
    const std::vector<Float>& _yhat_committed,
    Float* _Hfwf_ptr ) const
{
    assert_true( _update != enums::Update::calc_one );

    const auto ncolsplus1 = _old_weights.size();

    assert_true( nrows_ * ncolsplus1 == _eta1.size() );
    assert_true( nrows_ * ncolsplus1 == _eta2.size() );
    assert_true( nrows_ * ncolsplus1 == _eta1_old.size() );
    assert_true( nrows_ * ncolsplus1 == _eta2_old.size() );

    assert_true( h_.size() == nrows_ );
    assert_true( _yhat_committed.size() == nrows_ );

    if ( _update == enums::Update::calc_all )
        {
            _Hfwf_ptr[0] = -sum_h_yhat_committed_;
        }

    for ( const auto ix : _indices_current )
        {
            assert_true( ix < nrows_ );

            const auto w_old = calc_w_old(
                _old_weights,
                &_eta1[ncolsplus1 * ix],
                &_eta2[ncolsplus1 * ix] );

            assert_true( !std::isnan( w_old ) && !std::isinf( w_old ) );

            const auto h_sample_weights = h_[ix] * sample_weights( ix );

            if ( _update == enums::Update::calc_all )
                {
                    _Hfwf_ptr[0] += h_sample_weights * w_old;
                }

            const auto w_fixed = _yhat_committed[ix] - w_old;

            update_Hfwf(
                ncolsplus1,
                h_sample_weights,
                w_fixed,
                &_eta1[ncolsplus1 * ix],
                &_eta1_old[ncolsplus1 * ix],
                &_eta2[ncolsplus1 * ix],
                &_eta2_old[ncolsplus1 * ix],
                _Hfwf_ptr );
        }
}

// ----------------------------------------------------------------------------

void LossFunctionImpl::calc_H_ptr(
    const enums::Update _update,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta1_old,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _eta2_old,
    Float* _H_ptr ) const
{
    assert_true( nrows_ > 0 );
    assert_true( _eta1.size() % nrows_ == 0 );

    const auto ncolsplus1 = _eta1.size() / nrows_;

    const auto dim = calc_dim( ncolsplus1, _update );

    if ( _update != enums::Update::calc_diff )
        {
            _H_ptr[0] = sum_h_;

            for ( size_t j = 1; j < dim; ++j )
                {
                    _H_ptr[j * ( j + 1 ) / 2 + j] =
                        hyperparameters().reg_lambda_;
                }
        }

    auto old_vals = std::vector<Float>( dim );

    auto new_vals = std::vector<Float>( dim );

    old_vals[0] = new_vals[0] = 1.0;

    for ( const auto ix : _indices_current )
        {
            assert_true( ix < targets().size() );

            for ( size_t j = 0; j < ncolsplus1; ++j )
                {
                    old_vals[j + 1] = _eta1_old[ix * ncolsplus1 + j];
                    new_vals[j + 1] = _eta1[ix * ncolsplus1 + j];
                }

            if ( _update != enums::Update::calc_one )
                {
                    for ( size_t j = 0; j < ncolsplus1; ++j )
                        {
                            old_vals[j + ncolsplus1 + 1] =
                                _eta2_old[ix * ncolsplus1 + j];
                            new_vals[j + ncolsplus1 + 1] =
                                _eta2[ix * ncolsplus1 + j];
                        }
                }

            assert_true( std::all_of(
                old_vals.begin(), old_vals.end(), []( const Float val ) {
                    return !std::isnan( val ) && !std::isinf( val );
                } ) );

            assert_true( std::all_of(
                new_vals.begin(), new_vals.end(), []( const Float val ) {
                    return !std::isnan( val ) && !std::isinf( val );
                } ) );

            const auto h_sample_weights = h_[ix] * sample_weights( ix );

            for ( size_t j = 0; j < dim; ++j )
                {
                    for ( size_t k = 0; k <= j; ++k )
                        {
                            _H_ptr[j * ( j + 1 ) / 2 + k] +=
                                h_sample_weights *
                                ( new_vals[j] * new_vals[k] -
                                  old_vals[j] * old_vals[k] );
                        }
                }
        }
}

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

std::pair<Float, containers::Weights> LossFunctionImpl::calc_pair(
    const enums::Update _update,
    const std::vector<Float>& _old_weights,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta1_old,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _eta2_old,
    const std::vector<Float>& _yhat_committed,
    std::vector<Float>* _sufficient_stats,
    multithreading::Communicator* _comm ) const
{
    // ------------------------------------------------------------------------

    calc_sufficient_stats(
        _update,
        _old_weights,
        _indices_current,
        _eta1,
        _eta1_old,
        _eta2,
        _eta2_old,
        _yhat_committed,
        _sufficient_stats );

    //------------------------------------------------------------------------

    auto sufficient_stats_global = *_sufficient_stats;

    utils::Reducer::reduce<Float>(
        std::plus<Float>(), &sufficient_stats_global, _comm );

    // ------------------------------------------------------------------------

    const auto ncolsplus1 = static_cast<int>( _old_weights.size() );

    const auto [partial_loss, weights] =
        calc_results( _update, _old_weights.size(), sufficient_stats_global );

    //------------------------------------------------------------------------

    const Float intercept = weights( 0 );

    auto weights1 = std::vector<Float>( ncolsplus1 );

    for ( int i = 0; i < ncolsplus1; ++i )
        {
            weights1[i] = weights( i + 1 );
        }

    auto weights2 = std::vector<Float>( ncolsplus1 );

    if ( _update != enums::Update::calc_one )
        {
            for ( int i = 0; i < ncolsplus1; ++i )
                {
                    weights2[i] = weights( ncolsplus1 + i + 1 );
                }
        }

    auto weights_tuple = std::make_tuple(
        intercept, std::move( weights1 ), std::move( weights2 ) );

    //------------------------------------------------------------------------

    return std::make_pair( partial_loss, std::move( weights_tuple ) );

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

void LossFunctionImpl::calc_sufficient_stats(
    const enums::Update _update,
    const std::vector<Float>& _old_weights,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta1_old,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _eta2_old,
    const std::vector<Float>& _yhat_committed,
    std::vector<Float>* _sufficient_stats ) const
{
    // ------------------------------------------------------------------------

    assert_true( nrows_ > 0 );
    assert_true( _eta1.size() % nrows_ == 0 );

    const auto ncolsplus1 = _old_weights.size();

    // ------------------------------------------------------------------------

    assert_true( nrows_ == targets().size() );

    assert_true( _eta1.size() == nrows_ * ncolsplus1 );
    assert_true( _eta2.size() == nrows_ * ncolsplus1 );
    assert_true( _eta1_old.size() == nrows_ * ncolsplus1 );
    assert_true( _eta2_old.size() == nrows_ * ncolsplus1 );
    assert_true( g_.size() == nrows_ );
    assert_true( h_.size() == nrows_ );

    assert_true( sample_weights_ );
    assert_true( sample_weights_->size() == targets().size() );

    // ------------------------------------------------------------------------

    const auto dim = calc_dim( ncolsplus1, _update );

    // ------------------------------------------------------------------------

    const auto size = 2 * dim + ( dim * ( dim + 1 ) ) / 2;

    if ( _update != enums::Update::calc_diff )
        {
            *_sufficient_stats = std::vector<Float>( size );
        }

    assert_true( _sufficient_stats->size() == size );

    // ------------------------------------------------------------------------

    const auto g_ptr = _sufficient_stats->data();

    const auto Hfwf_ptr = _sufficient_stats->data() + dim;

    const auto H_ptr = _sufficient_stats->data() + 2 * dim;

    // ------------------------------------------------------------------------

    calc_g_ptr( _indices_current, _eta1, _eta1_old, _eta2, _eta2_old, g_ptr );

    // If _update == calc_one, that means we are calculating the weights for the
    // root node. There can be no fixed weights.
    if ( _update != enums::Update::calc_one )
        {
            calc_Hfwf_ptr(
                _update,
                _old_weights,
                _indices_current,
                _eta1,
                _eta1_old,
                _eta2,
                _eta2_old,
                _yhat_committed,
                Hfwf_ptr );
        }

    calc_H_ptr(
        _update, _indices_current, _eta1, _eta1_old, _eta2, _eta2_old, H_ptr );

    // ------------------------------------------------------------------------
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

std::pair<Float, Eigen::VectorXd> LossFunctionImpl::calc_results(
    const enums::Update _update,
    const size_t _ncolsplus1,
    const std::vector<Float>& _sufficient_stats_global ) const
{
    // ------------------------------------------------------------------------

    const auto dim = calc_dim( _ncolsplus1, _update );

    const auto g_ptr = _sufficient_stats_global.data();

    const auto Hfwf_ptr = _sufficient_stats_global.data() + dim;

    const auto H_ptr = _sufficient_stats_global.data() + 2 * dim;

    // ------------------------------------------------------------------------

    Eigen::VectorXd g( dim );

    for ( int i = 0; i < dim; ++i )
        {
            assert_true( !std::isnan( g_ptr[i] ) && !std::isinf( g_ptr[i] ) );
            g( i ) = g_ptr[i];
        }

    // ------------------------------------------------------------------------

    Eigen::VectorXd Hfwf( dim );

    for ( int i = 0; i < dim; ++i )
        {
            assert_true(
                !std::isnan( Hfwf_ptr[i] ) && !std::isinf( Hfwf_ptr[i] ) );
            Hfwf( i ) = Hfwf_ptr[i];
        }

    // ------------------------------------------------------------------------

    Eigen::MatrixXd H( dim, dim );

    for ( int i = 0; i < dim; ++i )
        {
            for ( int j = 0; j < i; ++j )
                {
                    H( i, j ) = H( j, i ) = H_ptr[i * ( i + 1 ) / 2 + j];

                    assert_true(
                        !std::isnan( H( i, j ) ) && !std::isinf( H( i, j ) ) );
                }

            H( i, i ) = H_ptr[i * ( i + 1 ) / 2 + i];

            assert_true( !std::isnan( H( i, i ) ) && !std::isinf( H( i, i ) ) );
        }

    //------------------------------------------------------------------------

    const Eigen::VectorXd weights =
        H.completeOrthogonalDecomposition().solve( g + Hfwf );

    //------------------------------------------------------------------------

    const Float partial_loss = -0.5 * ( g + Hfwf ).dot( weights );

    //------------------------------------------------------------------------

    return std::make_pair( partial_loss, weights );

    //------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void LossFunctionImpl::calc_yhat(
    const std::vector<Float>& _old_weights,
    const containers::Weights& _new_weights,
    const std::vector<size_t>& _indices,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _yhat_committed,
    std::vector<Float>* _yhat ) const
{
    assert_true( _yhat->size() == _yhat_committed.size() );
    assert_true( _yhat->size() == nrows_ );

    assert_true( _eta1.size() == _eta2.size() );

    assert_true( nrows_ > 0 );
    assert_true( _eta1.size() % nrows_ == 0 );

    const auto ncolsplus1 = _eta1.size() / nrows_;

    assert_true( _old_weights.size() == ncolsplus1 );
    assert_true( std::get<1>( _new_weights ).size() == ncolsplus1 );
    assert_true( std::get<2>( _new_weights ).size() == ncolsplus1 );

    for ( const auto ix : _indices )
        {
            assert_true( ix < nrows_ );

            auto pred = 0.0;

            for ( size_t j = 0; j < ncolsplus1; ++j )
                {
                    const auto ix2 = ix * ncolsplus1 + j;

                    pred += _eta1[ix2] * std::get<1>( _new_weights )[j] +
                            _eta2[ix2] * std::get<2>( _new_weights )[j] -
                            ( _eta1[ix2] + _eta2[ix2] ) * _old_weights[j];
                }

            ( *_yhat )[ix] = _yhat_committed[ix] + pred;
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
}  // namespace relmt
