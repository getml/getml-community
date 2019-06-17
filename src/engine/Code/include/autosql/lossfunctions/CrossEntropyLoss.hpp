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
    CrossEntropyLoss( multithreading::Communicator* _comm );

    ~CrossEntropyLoss() = default;

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

   public:
    std::string type() const final { return "CrossEntropyLoss"; }

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

    // -----------------------------------------

   private:
    // Communicator object
    multithreading::Communicator* comm_;

    // -----------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace autosql

#endif  // AUTOSQL_LOSSFUNCTIONS_CROSSENTROPYLOSS_HPP_
