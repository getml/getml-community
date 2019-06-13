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
    std::vector<std::vector<AUTOSQL_FLOAT>> calculate_residuals(
        const std::vector<std::vector<AUTOSQL_FLOAT>>& _yhat_old,
        const containers::DataFrameView& _y ) final;

    // This calculates the optimal update rate at which we need
    // to add _yhat to _yhat_old
    std::vector<AUTOSQL_FLOAT> calculate_update_rates(
        const std::vector<std::vector<AUTOSQL_FLOAT>>& _yhat_old,
        const std::vector<std::vector<AUTOSQL_FLOAT>>& _predictions,
        const containers::DataFrameView& _y,
        const std::vector<AUTOSQL_FLOAT>& _sample_weights ) final;

    // -----------------------------------------

    std::string type() const final { return "SquareLoss"; }

    // -----------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace autosql

#endif  // AUTOSQL_LOSSFUNCTIONS_SQUARELOSS_HPP_
