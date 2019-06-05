#ifndef AUTOSQL_LOSSFUNCTIONS_SQUARELOSS_HPP_
#define AUTOSQL_LOSSFUNCTIONS_SQUARELOSS_HPP_

namespace autosql
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

class SquareLoss : public LossFunction
{
   public:
    SquareLoss();

    ~SquareLoss() = default;

    // -----------------------------------------

    // This calculates the gradient of the loss function w.r.t.
    // the current prediction
    containers::Matrix<AUTOSQL_FLOAT> calculate_residuals(
        const containers::Matrix<AUTOSQL_FLOAT>& _yhat_old,
        const containers::DataFrameView& _y ) final;

    // This calculates the optimal update rates at which we need
    // to add _yhat to _yhat_old
    containers::Matrix<AUTOSQL_FLOAT> calculate_update_rates(
        const containers::Matrix<AUTOSQL_FLOAT>& _yhat_old,
        const containers::Matrix<AUTOSQL_FLOAT>& _f_t,
        const containers::DataFrameView& _y,
        const containers::Matrix<AUTOSQL_FLOAT>& _sample_weights ) final;

    // -----------------------------------------

    std::string type() final { return "SquareLoss"; }
};

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace autosql

#endif  // AUTOSQL_LOSSFUNCTIONS_SQUARELOSS_HPP_
