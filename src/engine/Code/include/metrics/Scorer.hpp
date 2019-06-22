#ifndef METRICS_SCORER_HPP_
#define METRICS_SCORER_HPP_

namespace metrics
{
// ----------------------------------------------------------------------------

struct Scorer
{
    /// Returns the metrics in the object.
    static Poco::JSON::Object get_metrics( const Poco::JSON::Object& _obj );

    /// Calculates scores.
    static Poco::JSON::Object score(
        const bool _is_classification,
        const Float* const _yhat,
        const size_t _yhat_nrows,
        const size_t _yhat_ncols,
        const Float* const _y,
        const size_t _y_nrows,
        const size_t _y_ncols );
};

// ----------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_SCORER_HPP_
