#include "relboost/lossfunctions/lossfunctions.hpp"

namespace relboost
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

std::vector<size_t> LossFunctionImpl::calc_sample_index(
    const std::shared_ptr<const std::vector<Float>>& _sample_weights ) const
{
    assert( _sample_weights );

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

    assert( !std::isnan( std::get<0>( _weights ) ) );
    assert( !std::isnan( _old_intercept ) );

    assert( _eta1.size() == _eta2.size() );
    assert( _eta1.size() == targets().size() );

    assert( sample_weights_ );
    assert( _eta1.size() == sample_weights_->size() );

    // ------------------------------------------------------------------------

    if ( hyperparameters().reg_lambda_ == 0.0 )
        {
            return 0.0;
        }

    // ------------------------------------------------------------------------

    Float regularization = 0.0;

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
    assert( _eta_old.size() == targets().size() );
    assert( _eta_new.size() == targets().size() );

    assert( sample_weights_ );
    assert( sample_weights_->size() == targets().size() );

    Float regularization = 0.0;

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
    const std::vector<size_t>& _sample_index,
    const std::vector<Float>& _sample_weights,
    Float* _sum_g,
    Float* _sum_h,
    Float* _sum_sample_weights,
    multithreading::Communicator* _comm ) const
{
    // ------------------------------------------------------------------------

    assert( g_.size() == _sample_weights.size() );
    assert( h_.size() == _sample_weights.size() );

    // ------------------------------------------------------------------------

    *_sum_g = 0.0;

    for ( auto ix : _sample_index )
        {
            assert( ix < _sample_weights.size() );
            *_sum_g += g_[ix] * _sample_weights[ix];
        }

    // ------------------------------------------------------------------------

    *_sum_h = 0.0;

    for ( auto ix : _sample_index )
        {
            assert( ix < _sample_weights.size() );
            *_sum_h += h_[ix] * _sample_weights[ix];
        }

    // ------------------------------------------------------------------------

    *_sum_sample_weights = 0.0;

    for ( auto ix : _sample_index )
        {
            assert( ix < _sample_weights.size() );
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

    assert( _yhat_old.size() == _predictions.size() );
    assert( _yhat_old.size() == targets().size() );

    assert( _yhat_old.size() == g_.size() );
    assert( _yhat_old.size() == h_.size() );

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

std::vector<std::array<Float, 3>> LossFunctionImpl::calc_weights(
    const enums::Update _update,
    const Float _old_weight,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _split_begin,
    const std::vector<const containers::Match*>::iterator _split_end,
    const std::vector<const containers::Match*>::iterator _end,
    multithreading::Communicator* _comm ) const
{
    // ------------------------------------------------------------------------
    // Note the minus!

    const auto calc_g = [this](
                            const Float init, const containers::Match* ptr ) {
        return init - g_[ptr->ix_output];
    };

    auto g2 = std::accumulate( _begin, _split_begin, 0.0, calc_g );

    const auto g1 = std::accumulate( _split_begin, _split_end, 0.0, calc_g );

    g2 += std::accumulate( _split_end, _end, 0.0, calc_g );

    // ------------------------------------------------------------------------

    const auto calc_h = [this](
                            const Float init, const containers::Match* ptr ) {
        return init +
               h_[ptr->ix_output] * ( 1.0 + hyperparameters().reg_lambda_ );
    };

    auto h2 = std::accumulate( _begin, _split_begin, 0.0, calc_h );

    const auto h1 = std::accumulate( _split_begin, _split_end, 0.0, calc_h );

    h2 += std::accumulate( _split_end, _end, 0.0, calc_h );

    // ------------------------------------------------------------------------
    // In this case, it is impossible for the weights to be NAN.

    const auto arr = std::array<Float, 3>( {0.0, g1 / h1, g2 / h2} );

    return std::vector<std::array<Float, 3>>( {arr} );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::array<Float, 3> LossFunctionImpl::calc_weights_avg_null(
    const enums::Aggregation _agg,
    const Float _old_weight,
    const std::vector<size_t>& _indices,
    const std::vector<Float>& _eta,
    const std::vector<Float>& _w_fixed,
    const std::vector<Float>& _yhat_committed,
    multithreading::Communicator* _comm ) const
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

    std::array<Float, 2> g_eta_arr = {0.0, 0.0};

    // The intercept term.
    g_eta_arr[0] = -sum_g_;

    for ( const auto ix : _indices )
        {
            assert( ix < targets().size() );
            g_eta_arr[1] -= g_[ix] * _eta[ix] * sample_weights( ix );
        }

    // ------------------------------------------------------------------------
    // Calculate h_w_const.

    std::array<Float, 2> h_w_const_arr = {0.0, 0.0};

    h_w_const_arr[0] = -sum_h_yhat_committed_;

    for ( const auto ix : _indices )
        {
            assert( !std::isnan( _w_fixed[ix] ) );
            assert( ix < targets().size() );

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
            assert( ix < targets().size() );

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

    // ------------------------------------------------------------------------

    if ( _agg == enums::Aggregation::avg_first_null )
        {
            return std::array<Float, 3>( {weights[0], NAN, weights[1]} );
        }
    else if ( _agg == enums::Aggregation::avg_second_null )
        {
            return std::array<Float, 3>( {weights[0], weights[1], NAN} );
        }
    else
        {
            assert( false && "Aggregation type not known!" );

            return std::array<Float, 3>( {NAN, NAN, NAN} );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::array<Float, 3> LossFunctionImpl::calc_weights(
    const Float _old_weight,
    const std::vector<size_t>& _indices,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _yhat_committed,
    multithreading::Communicator* _comm ) const
{
    // ------------------------------------------------------------------------

    assert( _eta1.size() == targets().size() );
    assert( _eta2.size() == targets().size() );
    assert( g_.size() == targets().size() );
    assert( h_.size() == targets().size() );

    assert( sample_weights_ );
    assert( sample_weights_->size() == targets().size() );

    // ------------------------------------------------------------------------
    // Calculate g_eta_arr.

    std::array<Float, 3> g_eta_arr = {0.0, 0.0, 0.0};

    // The intercept term.
    g_eta_arr[0] = -sum_g_;

    for ( const auto ix : _indices )
        {
            assert( ix < targets().size() );
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

            assert( sample_weights( ix ) > 0.0 );

            assert( ix < targets().size() );
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
            assert( ix < targets().size() );

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

    auto weights = A.fullPivLu().solve( b );

    // ------------------------------------------------------------------------

    return std::array<Float, 3>( {weights[0], weights[1], weights[2]} );

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

Float LossFunctionImpl::commit(
    const std::vector<size_t>& _indices,
    const std::vector<Float>& _yhat,
    std::vector<Float>* _yhat_committed ) const
{
    assert( _yhat_committed->size() == _yhat.size() );
    assert( _yhat_committed->size() == h_.size() );

    Float sum_h_yhat = sum_h_yhat_committed_;

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
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _end,
    const std::vector<Float>& _weights,
    std::vector<Float>* _predictions ) const
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
