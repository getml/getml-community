#ifndef METRICS_METRIC_HPP_
#define METRICS_METRIC_HPP_

namespace metrics
{
// ----------------------------------------------------------------------------

class Metric
{
   public:
    Metric() {}

    virtual ~Metric() = default;

    // -----------------------------------------

    /// This calculates the score based on the predictions _yhat
    /// and the targets _y.
    virtual Poco::JSON::Object score(
        const Float* const _yhat,
        const size_t _yhat_nrows,
        const size_t _yhat_ncols,
        const Float* const _y,
        const size_t _y_nrows,
        const size_t _y_ncols ) = 0;

    // -----------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_METRIC_HPP_
