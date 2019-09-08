#ifndef OPTIMIZERS_ADAGRAD_HPP_
#define OPTIMIZERS_ADAGRAD_HPP_

namespace optimizers
{
// ----------------------------------------------------------------------------

class AdaGrad : public Optimizer
{
   public:
    AdaGrad(
        const Float _learning_rate, const Float _offset, const size_t _size )
        : learning_rate_( _learning_rate ),
          offset_( _offset ),
          sum_squared_gradients_( std::vector<Float>( _size ) )
    {
    }

    ~AdaGrad() = default;

    // ----------------------------------------------------------------------------

    /// This member functions implements the updating strategy of the optimizer.
    void update_weights(
        const Float _epoch_num,
        const std::vector<Float>& _gradients,
        std::vector<Float>* _weights ) final
    {
        assert_true( _gradients.size() == _weights->size() );
        assert_true( _gradients.size() == sum_squared_gradients_.size() );

        for ( size_t i = 0; i < _weights->size(); ++i )
            {
                update(
                    _gradients[i],
                    &sum_squared_gradients_[i],
                    &( *_weights )[i] );
            }
    }

    // ----------------------------------------------------------------------------

   private:
    // Does the actual AdaGrad updates.
    void update( const Float _g, Float* _sum, Float* _w ) const
    {
        *_sum += _g * _g;
        *_w -= learning_rate_ * _g / std::sqrt( *_sum + offset_ );
    };

    // ----------------------------------------------------------------------------

   private:
    /// The learning rate used for the AdaGrad algorithm.
    const Float learning_rate_;

    /// The offset prevents us from dividing by zero.
    const Float offset_;

    // Dividing by the sum of squared gradients is the core idea of the AdaGrad
    // algorithm.
    std::vector<Float> sum_squared_gradients_;
};

// ----------------------------------------------------------------------------
}  // namespace optimizers

#endif  // OPTIMIZERS_ADAGRAD_HPP_
