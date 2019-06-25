#ifndef METRICS_ACCURACY_HPP_
#define METRICS_ACCURACY_HPP_

namespace metrics
{
// ----------------------------------------------------------------------------

class Accuracy : public Metric
{
   public:
    Accuracy() {}

    Accuracy( multithreading::Communicator* _comm ) : impl_( _comm ) {}

    ~Accuracy() = default;

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
    Float yhat( size_t _i, size_t _j ) const { return impl_.yhat( _i, _j ); }

    /// Trivial getter
    Float y( size_t _i, size_t _j ) const { return impl_.y( _i, _j ); }

    // ------------------------------------------------------------------------

   private:
    /// Contains all the relevant data.
    MetricImpl impl_;

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_ACCURACY_HPP_
