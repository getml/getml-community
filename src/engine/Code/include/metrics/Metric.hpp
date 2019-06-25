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
        const Features _yhat, const Features _y ) = 0;

    // -----------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_METRIC_HPP_
