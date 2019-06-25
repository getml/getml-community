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
        const Features _yhat,
        const Features _y );
};

// ----------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_SCORER_HPP_
