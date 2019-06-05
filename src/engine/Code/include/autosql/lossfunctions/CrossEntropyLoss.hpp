#ifndef AUTOSQL_LOSSFUNCTIONS_CROSSENTROPYLOSS_HPP_
#define AUTOSQL_LOSSFUNCTIONS_CROSSENTROPYLOSS_HPP_

namespace autosql
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

class CrossEntropyLoss : public LossFunction
{
   public:
    CrossEntropyLoss();

    ~CrossEntropyLoss() = default;

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

   public:
    std::string type() final { return "CrossEntropyLoss"; }

    // -----------------------------------------

   private:
    /// Applies the logistic function.
    AUTOSQL_FLOAT logistic_function( const AUTOSQL_FLOAT& _val )
    {
        AUTOSQL_FLOAT result = 1.0 / ( 1.0 + exp( ( -1.0 ) * _val ) );

        if ( std::isnan( result ) || std::isinf( result ) )
            {
                if ( _val > 0.0 )
                    {
                        result = 1.0;
                    }
                else
                    {
                        result = 0.0;
                    }
            }

        return result;
    }
};

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace autosql

#endif  // AUTOSQL_LOSSFUNCTIONS_CROSSENTROPYLOSS_HPP_
