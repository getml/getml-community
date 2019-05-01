#ifndef METRICS_MAE_HPP_
#define METRICS_MAE_HPP_

namespace metrics
{
// ----------------------------------------------------------------------------

class MAE : public Metric
{
   public:
    MAE( multithreading::Communicator* _comm )
        : comm_( _comm ),
          ncols_( 0 ),
          nrows_( 0 ),
          y_( nullptr ),
          yhat_( nullptr )
    {
    }

    MAE() : MAE( nullptr ) {}

    ~MAE() = default;

    // ------------------------------------------------------------------------

    /// This calculates the loss based on the predictions _yhat
    /// and the targets _y.
    Poco::JSON::Object score(
        const METRICS_FLOAT* const _yhat,
        const size_t _yhat_nrows,
        const size_t _yhat_ncols,
        const METRICS_FLOAT* const _y,
        const size_t _y_nrows,
        const size_t _y_ncols ) final;

    // -----------------------------------------

   private:
    /// Trivial getter
    multithreading::Communicator& comm()
    {
        assert( comm_ != nullptr );
        return *( comm_ );
    }

    /// Trivial getter
    METRICS_FLOAT yhat( size_t _i, size_t _j ) const
    {
        return yhat_[_i * ncols_ + _j];
    }

    /// Trivial getter
    METRICS_FLOAT y( size_t _i, size_t _j ) const
    {
        return y_[_i * ncols_ + _j];
    }

    // -----------------------------------------

   private:
    /// Communicator object - for parallel versions only.
    multithreading::Communicator* comm_;

    /// Number of columns.
    size_t ncols_;

    /// Number of rows.
    size_t nrows_;

    /// Pointer to ground truth.
    const METRICS_FLOAT* y_;

    /// Pointer to predictions.
    const METRICS_FLOAT* yhat_;

    // -----------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_MAE_HPP_
