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
    virtual containers::Matrix<AUTOSQL_FLOAT> calculate_residuals(
        const containers::Matrix<AUTOSQL_FLOAT>& _yhat_old,
        const containers::DataFrameView& _y ) = 0;

    // This calculates the optimal update rate at which we need
    // to add _yhat to _yhat_old
    virtual containers::Matrix<AUTOSQL_FLOAT> calculate_update_rates(
        const containers::Matrix<AUTOSQL_FLOAT>& _yhat_old,
        const containers::Matrix<AUTOSQL_FLOAT>& _f_t,
        const containers::DataFrameView& _y,
        const containers::Matrix<AUTOSQL_FLOAT>& _sample_weights ) = 0;

    // This calculates the loss based on the predictions _yhat
    // and the targets _y. Since scores are supposed to be maximized,
    // this multiplies the loss with -1.
   /* virtual AUTOSQL_FLOAT score(
        const containers::Matrix<AUTOSQL_FLOAT>& _yhat,
        const containers::DataFrameView& _y ) = 0;*/

    // Returns a string describing this loss functioni
    virtual std::string type() = 0;

    // -----------------------------------------

#ifdef AUTOSQL_PARALLEL

    inline multithreading::Communicator& comm() { return *( comm_ ); }

    inline void set_comm( multithreading::Communicator* _comm ) { comm_ = _comm; }

#endif  // AUTOSQL_PARALLEL

        // -----------------------------------------

#ifdef AUTOSQL_PARALLEL

   protected:
    // Communicator object
    multithreading::Communicator* comm_;

#endif  // AUTOSQL_PARALLEL
};

// ----------------------------------------------------------------------------
}
}
#endif // AUTOSQL_LOSSFUNCTIONS_LOSSFUNCTION_HPP_ 
