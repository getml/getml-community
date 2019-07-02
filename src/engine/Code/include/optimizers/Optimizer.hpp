#ifndef OPTIMIZERS_OPTIMIZER_HPP_
#define OPTIMIZERS_OPTIMIZER_HPP_

namespace optimizers
{
// ----------------------------------------------------------------------------

class Optimizer
{
    /// This member functions implements the updating strategy of the optimizer.
    virtual void update_weights(
        const Float _epoch_num,
        const std::vector<Float>& _gradients,
        std::vector<Float>* _weights ) = 0;
};

// ----------------------------------------------------------------------------
}  // namespace optimizers

#endif  // OPTIMIZERS_OPTIMIZER_HPP_
