#include "lossfunctions/lossfunctions.hpp"

namespace relboost
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

void SquareLoss::calc_gradients(
    const std::shared_ptr<const std::vector<RELBOOST_FLOAT>>& _yhat_old )
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
        []( const RELBOOST_FLOAT& yhat, const RELBOOST_FLOAT& y ) {
            return yhat - y;
        } );

    // ------------------------------------------------------------------------
    // Calculate h_.

    std::fill( h_.begin(), h_.end(), 1.0 );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

RELBOOST_FLOAT SquareLoss::calc_loss(
    const std::array<RELBOOST_FLOAT, 3>& _weights )
{
    // ------------------------------------------------------------------------

    assert( yhat_.size() == targets().size() );

    assert( sample_weights_ );
    assert( yhat_.size() == sample_weights_->size() );

    // ------------------------------------------------------------------------

    assert( !std::isnan( std::get<0>( _weights ) ) );

    const auto num_targets = static_cast<RELBOOST_FLOAT>( targets().size() );

    RELBOOST_FLOAT loss = 0.0;

    for ( size_t i = 0; i < yhat_.size(); ++i )
        {
            auto diff = yhat_old()[i] + yhat_[i] + std::get<0>( _weights ) -
                        targets()[i];

            loss += diff * diff * ( *sample_weights_ )[i];
        }

    loss /= num_targets;

    // ------------------------------------------------------------------------

    return loss;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

RELBOOST_FLOAT SquareLoss::evaluate_split(
    const RELBOOST_FLOAT _old_intercept,
    const RELBOOST_FLOAT _old_weight,
    const std::array<RELBOOST_FLOAT, 3>& _weights )
{
    /*  //
      ------------------------------------------------------------------------

      auto new_weight = std::get<1>( _weights );

      //
      ------------------------------------------------------------------------

      const auto loss_function = [this, &new_weight, _old_weight](
                                     RELBOOST_FLOAT init,
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

RELBOOST_FLOAT SquareLoss::evaluate_tree(
    const std::vector<RELBOOST_FLOAT>& _yhat_new )
{
    assert( _yhat_new.size() == targets().size() );

    RELBOOST_FLOAT loss = 0.0;

    for ( size_t i = 0; i < _yhat_new.size(); ++i )
        {
            const auto diff = _yhat_new[i] - targets()[i];

            loss += diff * diff * ( *sample_weights_ )[i];
        }

    return loss;
}

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost
