#include "relboost/lossfunctions/lossfunctions.hpp"

namespace relboost
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

void CrossEntropyLoss::calc_gradients(
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
        [this]( const RELBOOST_FLOAT& yhat, const RELBOOST_FLOAT& y ) {
            return logistic_function( yhat ) - y;
        } );

    // ------------------------------------------------------------------------
    // Calculate h_.

    std::transform(
        yhat_old().begin(),
        yhat_old().end(),
        h_.begin(),
        [this]( const RELBOOST_FLOAT& yhat ) {
            const auto sigma_yhat = logistic_function( yhat );
            return sigma_yhat * ( 1.0 - sigma_yhat );
        } );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

RELBOOST_FLOAT CrossEntropyLoss::calc_loss(
    const std::array<RELBOOST_FLOAT, 3>& _weights )
{
    // ------------------------------------------------------------------------

    assert( yhat_.size() == targets().size() );

    assert( sample_weights_ );
    assert( yhat_.size() == sample_weights_->size() );

    // ------------------------------------------------------------------------

    assert( !std::isnan( std::get<0>( _weights ) ) );

    RELBOOST_FLOAT loss = 0.0;

    for ( size_t ix : sample_index_ )
        {
            assert( ix < yhat_.size() );

            const auto sigma_yhat = logistic_function(
                std::get<0>( _weights ) + yhat_old()[ix] + yhat_[ix] );

            loss += log_loss( sigma_yhat, targets()[ix] ) *
                    ( *sample_weights_ )[ix];
        }

    // ------------------------------------------------------------------------

    utils::Reducer::reduce( std::plus<RELBOOST_FLOAT>(), &loss, &comm() );

    // ------------------------------------------------------------------------

#ifndef NDEBUG

    auto global_sum_sample_weights = sum_sample_weights_;

    utils::Reducer::reduce(
        multithreading::maximum<RELBOOST_FLOAT>(),
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

    return loss;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

RELBOOST_FLOAT CrossEntropyLoss::evaluate_split(
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

RELBOOST_FLOAT CrossEntropyLoss::evaluate_tree(
    const std::vector<RELBOOST_FLOAT>& _yhat_new )
{
    assert( _yhat_new.size() == targets().size() );

    RELBOOST_FLOAT loss = 0.0;

    for ( size_t ix : sample_index_ )
        {
            const auto sigma_yhat = logistic_function( _yhat_new[ix] );

            loss += log_loss( sigma_yhat, targets()[ix] ) *
                    ( *sample_weights_ )[ix];
        }

    utils::Reducer::reduce( std::plus<RELBOOST_FLOAT>(), &loss, &comm() );

    return loss;
}

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost
