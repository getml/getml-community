#ifndef METRICS_METRICIMPL_HPP_
#define METRICS_METRICIMPL_HPP_

namespace metrics
{
// ----------------------------------------------------------------------------

class MetricImpl
{
   public:
    MetricImpl() : MetricImpl( nullptr ) {}

    MetricImpl( multithreading::Communicator* _comm )
        : comm_( _comm ),
          ncols_( 0 ),
          nrows_( 0 ),
          y_( nullptr ),
          yhat_( nullptr )
    {
    }

    ~MetricImpl() = default;

    // -----------------------------------------

   public:
    /// Trivial getter
    multithreading::Communicator& comm()
    {
        assert( has_comm() );
        return *( comm_ );
    }

    /// Whether there is a communicator.
    bool has_comm() const { return comm_ != nullptr; }

    /// Trivial getter
    size_t ncols() const { return ncols_; }

    /// Trivial getter
    size_t nrows() const { return nrows_; }

    /// Reduces a value in a multithreading context.
    template <typename OperatorType>
    void reduce( const OperatorType& _operator, METRICS_FLOAT* _val )
    {
        METRICS_FLOAT global = 0.0;

        multithreading::all_reduce(
            comm(),    // comm
            _val,      // in_values
            1,         // count
            &global,   // out_values
            _operator  // op
        );

        comm().barrier();

        *_val = global;
    }

    /// Reduces a vector in a multithreading context.
    template <typename OperatorType>
    void reduce(
        const OperatorType& _operator, std::vector<METRICS_FLOAT>* _vec )
    {
        std::vector<METRICS_FLOAT> global( _vec->size() );

        multithreading::all_reduce(
            comm(),         // comm
            _vec->data(),    // in_values
            _vec->size(),    // count,
            global.data(),  // out_values
            _operator       // op
        );

        comm().barrier();

        *_vec = std::move( global );
    }

    /// Trivial setter
    void set_data(
        const METRICS_FLOAT* const _yhat,
        const size_t _yhat_nrows,
        const size_t _yhat_ncols,
        const METRICS_FLOAT* const _y,
        const size_t _y_nrows,
        const size_t _y_ncols )
    {
        assert( _yhat_nrows == _y_nrows );
        assert( _yhat_ncols == _y_ncols );

        nrows_ = _yhat_nrows;
        ncols_ = _yhat_ncols;
        yhat_ = _yhat;
        y_ = _y;
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

#endif  // METRICS_METRICIMPL_HPP_
