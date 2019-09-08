#ifndef METRICS_METRICIMPL_HPP_
#define METRICS_METRICIMPL_HPP_

namespace metrics
{
// ----------------------------------------------------------------------------

class MetricImpl
{
   public:
    MetricImpl() : MetricImpl( nullptr ) {}

    MetricImpl( multithreading::Communicator* _comm ) : comm_( _comm ) {}

    ~MetricImpl() = default;

    // -----------------------------------------

   public:
    /// Trivial getter
    multithreading::Communicator& comm()
    {
        assert_true( has_comm() );
        return *( comm_ );
    }

    /// Whether there is a communicator.
    bool has_comm() const { return comm_ != nullptr; }

    /// Trivial getter
    size_t ncols() const { return y_.size(); }

    /// Trivial getter
    size_t nrows() const
    {
        return ( ncols() == 0 ) ? ( 0 ) : ( y_[0]->size() );
    }

    /// Reduces a value in a multithreading context.
    template <typename OperatorType>
    void reduce( const OperatorType& _operator, Float* _val )
    {
        Float global = 0.0;

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
    void reduce( const OperatorType& _operator, std::vector<Float>* _vec )
    {
        std::vector<Float> global( _vec->size() );

        multithreading::all_reduce(
            comm(),         // comm
            _vec->data(),   // in_values
            _vec->size(),   // count,
            global.data(),  // out_values
            _operator       // op
        );

        comm().barrier();

        *_vec = std::move( global );
    }

    /// Trivial setter
    void set_data( const Features _yhat, const Features _y )
    {
        assert_true( _yhat.size() == _y.size() );

        for ( size_t i = 0; i < _y.size(); ++i )
            {
                assert_true( _y[i]->size() == _yhat[i]->size() );
                assert_true( _y[i]->size() == _y[0]->size() );
            }

        yhat_ = _yhat;
        y_ = _y;
    }

    /// Trivial getter
    Float y( size_t _i, size_t _j ) const
    {
        assert_true( _j < y_.size() );
        assert_true( y_[_j] );
        assert_true( _i < y_[_j]->size() );

        return ( *y_[_j] )[_i];
    }

    /// Trivial getter
    Float yhat( size_t _i, size_t _j ) const
    {
        assert_true( _j < yhat_.size() );
        assert_true( yhat_[_j] );
        assert_true( _i < yhat_[_j]->size() );

        return ( *yhat_[_j] )[_i];
    }

    // -----------------------------------------

   private:
    /// Communicator object - for parallel versions only.
    multithreading::Communicator* comm_;

    /// Ground truth.
    Features y_;

    /// Predictions.
    Features yhat_;

    // -----------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_METRICIMPL_HPP_
