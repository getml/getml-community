#ifndef METRICS_AUC_HPP_
#define METRICS_AUC_HPP_

namespace metrics
{
// ----------------------------------------------------------------------------

class AUC : public Metric
{
   public:
    AUC() {}

    AUC( multithreading::Communicator* _comm ) : impl_( _comm ) {}

    ~AUC() = default;

    // ------------------------------------------------------------------------

   public:
    /// This calculates the loss based on the predictions _yhat
    /// and the targets _y.
    Poco::JSON::Object score( const Features _yhat, const Features _y ) final;

    // ------------------------------------------------------------------------

   private:
    Float calc_auc(
        const std::vector<Float>& _true_positive_rate,
        const std::vector<Float>& _false_positive_rate ) const;

    std::vector<Float> calc_false_positives(
        const std::vector<Float>& _true_positives,
        const std::vector<Float>& _predicted_negative ) const;

    std::vector<Float> calc_rate(
        const std::vector<Float>& _raw, const Float _all ) const;

    std::pair<std::vector<Float>, Float> calc_true_positives_uncompressed(
        const std::vector<std::pair<Float, Float>>& _pairs ) const;

    std::pair<std::vector<Float>, std::vector<Float>> compress(
        const std::vector<std::pair<Float, Float>>& _pairs,
        const std::vector<Float>& _true_positives_uncompressed,
        const Float _all_positives ) const;

    std::vector<Float> downsample( const std::vector<Float>& _original ) const;

    std::pair<Float, Float> find_min_max( const size_t _j ) const;

    std::vector<std::pair<Float, Float>> make_pairs( const size_t _j ) const;

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

#endif  // METRICS_AUC_HPP_
