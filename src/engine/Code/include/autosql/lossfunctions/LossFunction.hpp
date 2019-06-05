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
    virtual containers::Matrix<SQLNET_FLOAT> calculate_residuals(
        const containers::Matrix<SQLNET_FLOAT>& _yhat_old,
        const containers::DataFrameView& _y ) = 0;

    // This calculates the optimal update rate at which we need
    // to add _yhat to _yhat_old
    virtual containers::Matrix<SQLNET_FLOAT> calculate_update_rates(
        const containers::Matrix<SQLNET_FLOAT>& _yhat_old,
        const containers::Matrix<SQLNET_FLOAT>& _f_t,
        const containers::DataFrameView& _y,
        const containers::Matrix<SQLNET_FLOAT>& _sample_weights ) = 0;

    // This calculates the loss based on the predictions _yhat
    // and the targets _y. Since scores are supposed to be maximized,
    // this multiplies the loss with -1.
   /* virtual SQLNET_FLOAT score(
        const containers::Matrix<SQLNET_FLOAT>& _yhat,
        const containers::DataFrameView& _y ) = 0;*/

    // Returns a string describing this loss functioni
    virtual std::string type() = 0;

    // -----------------------------------------

#ifdef SQLNET_PARALLEL

    inline SQLNET_COMMUNICATOR& comm() { return *( comm_ ); }

    inline void set_comm( SQLNET_COMMUNICATOR* _comm ) { comm_ = _comm; }

#endif  // SQLNET_PARALLEL

        // -----------------------------------------

#ifdef SQLNET_PARALLEL

   protected:
    // Communicator object
    SQLNET_COMMUNICATOR* comm_;

#endif  // SQLNET_PARALLEL
};

// ----------------------------------------------------------------------------
}
}
#endif // AUTOSQL_LOSSFUNCTIONS_LOSSFUNCTION_HPP_ 
