#ifndef METRICS_RSQUARED_HPP_
#define METRICS_RSQUARED_HPP_

namespace metrics
{
// ----------------------------------------------------------------------------

class RSquared : public Metric
{
   public:
    RSquared() {}

    RSquared( multithreading::Communicator* _comm ) : impl_( _comm ) {}

    ~RSquared() = default;

    // ------------------------------------------------------------------------

    /// This calculates the loss based on the predictions _yhat
    /// and the targets _y.
    Poco::JSON::Object score( const Features _yhat, const Features _y ) final;

    // ------------------------------------------------------------------------

   private:
    /// Trivial getter
    multithreading::Communicator& comm() { return impl_.comm(); }

    /// Trivial getter
    size_t ncols() const { return impl_.ncols(); }

    /// Trivial getter
    size_t nrows() const { return impl_.nrows(); }

    /// Trivial getter
    Float& sufficient_statistics( size_t _i, size_t _j )
    {
        assert_true( sufficient_statistics_.size() % ncols() == 0 );
        assert_true( _i < sufficient_statistics_.size() / ncols() );
        assert_true( _j < ncols() );

        return sufficient_statistics_[_i * ncols() + _j];
    }

    /// Trivial getter
    Float yhat( size_t _i, size_t _j ) const { return impl_.yhat( _i, _j ); }

    /// Trivial getter
    Float y( size_t _i, size_t _j ) const { return impl_.y( _i, _j ); }

    // ------------------------------------------------------------------------

   private:
    /// Contains all the relevant data.
    MetricImpl impl_;

    /// Sufficient statistics for calculating RSquared
    std::vector<Float> sufficient_statistics_;

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_RSQUARED_HPP_
