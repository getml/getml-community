#include "relboost/lossfunctions/lossfunctions.hpp"

namespace relboost
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

void SquareLoss::calc_gradients(
    const std::shared_ptr<const std::vector<Float>>& _yhat_old )
{
    // ------------------------------------------------------------------------

    assert( _yhat_old );
    assert( _yhat_old->size() == targets().size() );

    yhat_old_ = _yhat_old;

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
        []( const Float& yhat, const Float& y ) {
            return yhat - y;
        } );

    // ------------------------------------------------------------------------
    // Calculate h_.

    std::fill( h_.begin(), h_.end(), 1.0 );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Float SquareLoss::calc_loss(
    const std::array<Float, 3>& _weights )
{
    // ------------------------------------------------------------------------

    assert( yhat_.size() == targets().size() );

    assert( sample_weights_ );
    assert( yhat_.size() == sample_weights_->size() );

    // ------------------------------------------------------------------------

    assert( !std::isnan( std::get<0>( _weights ) ) );

    Float loss = 0.0;

    for ( size_t ix : sample_index_ )
        {
            assert( ix < yhat_.size() );

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
        multithreading::maximum<Float>(),
        &global_sum_sample_weights,
        &comm() );

    assert( global_sum_sample_weights == sum_sample_weights_ );

#endif  // NDEBUG

    // ------------------------------------------------------------------------

    assert( sum_sample_weights_ > 0.0 || sample_index_.size() == 0.0 );

    if ( sum_sample_weights_ > 0.0 )
        {
            loss /= sum_sample_weights_;
        }

        // ------------------------------------------------------------------------

#ifndef NDEBUG

    auto global_loss = loss;

    utils::Reducer::reduce(
        multithreading::maximum<Float>(), &global_loss, &comm() );

    assert( global_loss == loss );

#endif  // NDEBUG

    // ------------------------------------------------------------------------

    return loss;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Float SquareLoss::evaluate_split(
    const Float _old_intercept,
    const Float _old_weight,
    const std::array<Float, 3>& _weights )
{
    /*  //
      ------------------------------------------------------------------------

      auto new_weight = std::get<1>( _weights );

      //
      ------------------------------------------------------------------------

      const auto loss_function = [this, &new_weight, _old_weight](
                                     Float init,
                                     const containers::Match* ptr ) {
          const auto ix = ptr->ix_output;

          assert( ix < targets().size() );

          const auto diff_old = _old_weight - targets()[ix];

          const auto diff_new = new_weight - targets()[ix];

          return init + diff_old * diff_old - diff_new * diff_new;
      };

      //
      ------------------------------------------------------------------------

      auto result = std::accumulate( _split, _end, 0.0, loss_function );

      //
      ------------------------------------------------------------------------

      new_weight = std::get<2>( _weights );

      result += std::accumulate( _split, _end, 0.0, loss_function );

      //
      ------------------------------------------------------------------------

      return result;*/

    return 0.0;
}

// ----------------------------------------------------------------------------

Float SquareLoss::evaluate_tree(
    const std::vector<Float>& _yhat_new )
{
    assert( _yhat_new.size() == targets().size() );

    Float loss = 0.0;

    for ( size_t ix : sample_index_ )
        {
            const auto diff = _yhat_new[ix] - targets()[ix];

            loss += diff * diff * ( *sample_weights_ )[ix];
        }

    utils::Reducer::reduce( std::plus<Float>(), &loss, &comm() );

    return loss;
}

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost
