#include "relboost/lossfunctions/lossfunctions.hpp"

namespace relboost
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

void SquareLoss::calc_gradients()
{
    // ------------------------------------------------------------------------

    assert_true( yhat_old().size() == targets().size() );

    // ------------------------------------------------------------------------
    // Resize, if necessary

    if ( g_.size() != yhat_old().size() )
        {
            resize( yhat_old().size() );
        }

    // ------------------------------------------------------------------------
    // Calculate g_.

    std::transform(
        yhat_old().begin(),
        yhat_old().end(),
        targets().begin(),
        g_.begin(),
        []( const Float& yhat, const Float& y ) { return yhat - y; } );

    // ------------------------------------------------------------------------
    // Calculate h_.

    std::fill( h_.begin(), h_.end(), 1.0 );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Float SquareLoss::calc_loss( const std::array<Float, 3>& _weights )
{
    // ------------------------------------------------------------------------

    assert_true( yhat_.size() == targets().size() );

    assert_true( sample_weights_ );
    assert_true( yhat_.size() == sample_weights_->size() );

    // ------------------------------------------------------------------------

    assert_true( !std::isnan( std::get<0>( _weights ) ) );

    Float loss = 0.0;

    for ( size_t ix : sample_index_ )
        {
            assert_true( ix < yhat_.size() );

            auto diff = yhat_old()[ix] + yhat_[ix] + std::get<0>( _weights ) -
                        targets()[ix];

            loss += diff * diff * ( *sample_weights_ )[ix];
        }

    // ------------------------------------------------------------------------

    utils::Reducer::reduce( std::plus<Float>(), &loss, &comm() );

    // ------------------------------------------------------------------------

#ifndef NDEBUG

    auto global_sum_sample_weights = sum_sample_weights_;

    utils::Reducer::reduce(
        multithreading::maximum<Float>(), &global_sum_sample_weights, &comm() );

    assert_true( global_sum_sample_weights == sum_sample_weights_ );

#endif  // NDEBUG

    // ------------------------------------------------------------------------

    assert_true( sum_sample_weights_ > 0.0 || sample_index_.size() == 0.0 );

    if ( sum_sample_weights_ > 0.0 )
        {
            loss /= sum_sample_weights_;
        }

        // ------------------------------------------------------------------------

#ifndef NDEBUG

    auto global_loss = loss;

    utils::Reducer::reduce(
        multithreading::maximum<Float>(), &global_loss, &comm() );

    assert_true( global_loss == loss );

#endif  // NDEBUG

    // ------------------------------------------------------------------------

    return loss;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Float SquareLoss::evaluate_tree(
    const Float _update_rate, const std::vector<Float>& _predictions )
{
    assert_true( _predictions.size() == targets().size() );

    std::vector<Float> yhat_new( targets().size() );

    const auto update_function = [_update_rate](
                                     const Float y_old, const Float y_new ) {
        return y_old + y_new * _update_rate;
    };

    std::transform(
        yhat_old().begin(),
        yhat_old().end(),
        _predictions.begin(),
        yhat_new.begin(),
        update_function );

    Float loss = 0.0;

    for ( size_t ix : sample_index_ )
        {
            const auto diff = yhat_new[ix] - targets()[ix];

            loss += diff * diff * ( *sample_weights_ )[ix];
        }

    utils::Reducer::reduce( std::plus<Float>(), &loss, &comm() );

    return loss;
}

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost
