#ifndef AUTOSQL_LOSSFUNCTIONS_LOSSFUNCTION_HPP_
#define AUTOSQL_LOSSFUNCTIONS_LOSSFUNCTION_HPP_

namespace autosql
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

class LossFunction
{
   public:
    LossFunction() {}

    virtual ~LossFunction() = default;

    // -----------------------------------------

    // This calculates the gradient of the loss function w.r.t.
    // the current prediction
    virtual std::vector<std::vector<Float>> calculate_residuals(
        const std::vector<std::vector<Float>>& _yhat_old,
        const containers::DataFrameView& _y ) = 0;

    // This calculates the optimal update rate at which we need
    // to add _yhat to _yhat_old
    virtual std::vector<Float> calculate_update_rates(
        const std::vector<std::vector<Float>>& _yhat_old,
        const std::vector<std::vector<Float>>& _predictions,
        const containers::DataFrameView& _y,
        const std::vector<Float>& _sample_weights ) = 0;

    // Returns a string describing this loss functioni
    virtual std::string type() const = 0;

    // -----------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace autosql
#endif  // AUTOSQL_LOSSFUNCTIONS_LOSSFUNCTION_HPP_
