#ifndef MULTIREL_LOSSFUNCTIONS_SQUARELOSS_HPP_
#define MULTIREL_LOSSFUNCTIONS_SQUARELOSS_HPP_

namespace multirel
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

class SquareLoss : public LossFunction
{
   public:
    SquareLoss( multithreading::Communicator* _comm );

    ~SquareLoss() = default;

    // -----------------------------------------

    // This calculates the gradient of the loss function w.r.t.
    // the current prediction
    std::vector<std::vector<Float>> calculate_residuals(
        const std::vector<std::vector<Float>>& _yhat_old,
        const containers::DataFrameView& _y ) final;

    // This calculates the optimal update rate at which we need
    // to add _yhat to _yhat_old
    std::vector<Float> calculate_update_rates(
        const std::vector<std::vector<Float>>& _yhat_old,
        const std::vector<std::vector<Float>>& _predictions,
        const containers::DataFrameView& _y,
        const std::vector<Float>& _sample_weights ) final;

    // -----------------------------------------

    std::string type() const final { return "SquareLoss"; }

    // -----------------------------------------

   private:
    // Communicator object
    multithreading::Communicator* comm_;

    // -----------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace multirel

#endif  // MULTIREL_LOSSFUNCTIONS_SQUARELOSS_HPP_
